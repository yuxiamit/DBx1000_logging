#include <sched.h>
#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

int ycsb_wl::next_tid;

const char init_value[100] = "hello\n";

__thread char * row_memory;
__thread char * manager_mem;
__thread char * item_mem;
__thread char * index_node_mem;
__thread char * data_mem;


//ATTRIBUTE_NO_SANITIZE_ADDRESS
RC ycsb_wl::init() {
	workload::init();
	next_tid = 0;
	//char * cpath = getenv("GRAPHITE_HOME");
	string path;
	//if (cpath == NULL) 
	path = "./benchmarks/YCSB_schema.txt";
	//else { 
	//	path = string(cpath);
	//	path += "/tests/apps/dbms/YCSB_schema.txt";
	//}
	init_schema( path );
	
	init_table_parallel();
//	init_table();
	return RCOK;
}

RC ycsb_wl::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"]; 	
	the_index = hash_indexes["HASH_MAIN_INDEX"];
	return RCOK;
}
	
int 
ycsb_wl::key_to_part(uint64_t key) {
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return key / rows_per_part;
}

RC ycsb_wl::init_table() {
	RC rc;
    uint64_t total_row = 0;
    while (true) {
    	for (UInt32 part_id = 0; part_id < g_part_cnt; part_id ++) {
            if (total_row > g_synth_table_size)
                goto ins_done;
            row_t * new_row = NULL;
			uint64_t row_id;
            rc = the_table->get_new_row(new_row, part_id, row_id); 
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
			assert(rc == RCOK);
			uint64_t primary_key = total_row;
			new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
			Catalog * schema = the_table->get_schema();
			for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
				int field_size = schema->get_field_size(fid);
				char value[field_size];
				for (int i = 0; i < field_size; i++) 
					value[i] = (char)rand() % (1<<8) ;
				new_row->set_value(fid, value);
			}
            itemid_t * m_item = 
                (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
			assert(m_item != NULL);
            m_item->type = DT_row;
            m_item->location = new_row;
            m_item->valid = true;
            uint64_t idx_key = primary_key;
            rc = the_index->index_insert(idx_key, m_item, part_id);
            assert(rc == RCOK);
            total_row ++;
        }
    }
ins_done:
    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
    return RCOK;

}

uint64_t aligned(uint64_t size)
{
	return size / ALIGN_SIZE * ALIGN_SIZE + ALIGN_SIZE;
}

// init table in parallel
void ycsb_wl::init_table_parallel() {
	enable_thread_mem_pool = true;
	printf("g_init_parallelism: %u\n", g_init_parallelism);
	pthread_t p_thds[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitTable, this);
	threadInitTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
	mem_allocator.unregister();
}

void ycsb_wl::read_wl_from_file_parallel(uint32_t tid)
{
	/*uint64_t start = g_synth_table_size * tid / g_init_parallelism; 
	uint64_t end = g_synth_table_size * (tid + 1) / g_init_parallelism;

	size_t aligned_row_size = aligned(sizeof(row_t));
	size_t aligned_manager_size = aligned(row_t::get_manager_size());
	uint32_t disk_id = tid % g_num_logger;
	ifstream input("/data" + to_string(disk_id) + "/ycsb_table.dat." + to_string(tid));
	*/
	assert(false); // not implemented
}

void ycsb_wl::save_wl_to_file_parallel(uint32_t tid)
{
	uint64_t start = g_synth_table_size * tid / g_init_parallelism; 
	uint64_t end = g_synth_table_size * (tid + 1) / g_init_parallelism;

	size_t aligned_row_size = aligned(sizeof(row_t));
	size_t aligned_manager_size = aligned(row_t::get_manager_size());
	uint32_t disk_id = tid % g_num_logger;
	ofstream output("/data" + to_string(disk_id) + "/ycsb_table.dat." + to_string(tid));
	output.write(row_memory, (end-start) * aligned_row_size);
	output.write(manager_mem, (end-start) * aligned_manager_size);
	output.write(item_mem, (end - start) * sizeof(itemid_t));
	output.write(index_node_mem, (end - start) * sizeof(BucketNode));
	output.close();
}

__attribute__((no_sanitize_address))
void * ycsb_wl::init_table_slice() {
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
	// set cpu affinity
	set_affinity(tid);

	mem_allocator.register_thread(tid);
	RC rc;
	//assert(g_synth_table_size % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
	
	while ((UInt32)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
	assert((UInt32)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
	//uint64_t slice_size = g_synth_table_size / g_init_parallelism;
	uint64_t start = g_synth_table_size * tid / g_init_parallelism; 
	uint64_t end = g_synth_table_size * (tid + 1) / g_init_parallelism;

	uint64_t aligned_row_size = aligned(sizeof(row_t));
	uint64_t aligned_manager_size = aligned(row_t::get_manager_size());
	uint64_t aligned_data_size = aligned(the_table->get_schema()->get_tuple_size());
	row_memory = (char*) _mm_malloc((end-start) * aligned_row_size, ALIGN_SIZE);
	manager_mem = (char*) _mm_malloc((end-start) * aligned_manager_size, ALIGN_SIZE);
	item_mem = (char*) _mm_malloc((end - start) * sizeof(itemid_t), ALIGN_SIZE);
	// assert index is hash index
	index_node_mem = (char*) _mm_malloc((end - start) * sizeof(BucketNode), ALIGN_SIZE);
	data_mem = (char*)  _mm_malloc((end - start) * aligned_data_size, ALIGN_SIZE);
	assert(g_part_cnt == 1);
	// once for all

	//uint64_t step = (end - start) / 100;

	for (uint64_t key = start; key < end; key ++) {
		row_t * new_row = NULL;
		uint64_t row_id;
		int part_id = key_to_part(key);
		rc = the_table->get_new_row(new_row, part_id, row_id, 
			row_memory + (key - start) * aligned_row_size, 
			manager_mem + (key - start) * aligned_manager_size, 
			data_mem + (key - start) * aligned_data_size
		); 
		assert(rc == RCOK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, &primary_key);
		Catalog * schema = the_table->get_schema();
		for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
			new_row->set_value(fid, (void*)init_value);
		}

		itemid_t * m_item =
			(itemid_t *)  (item_mem + (key - start) * sizeof(itemid_t));// mem_allocator.alloc( sizeof(itemid_t), part_id );
		assert(m_item != NULL);
		m_item->type = DT_row;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;
		
		rc = the_index->index_insert(idx_key, m_item, part_id, index_node_mem + (key - start) * sizeof(BucketNode));
		assert(rc == RCOK);

		//if((key - start) % step == 0)
		//	printf("[%u] Initialize %lu percent\n", tid, (key - start) / step);
	}
	return NULL;
}

RC ycsb_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd){
	txn_manager = (ycsb_txn_man *)
		_mm_malloc( sizeof(ycsb_txn_man), 64 );
	new(txn_manager) ycsb_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}


