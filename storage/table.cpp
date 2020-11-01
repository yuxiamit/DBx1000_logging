#include "global.h"
#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"

void table_t::init(Catalog * schema) {
	this->table_name = schema->table_name;
	this->schema = schema;
	_primary_index = NULL; 

#if TPCC_PHANTOM_AVOIDANCE && TPCC_PHANTOM_AVOIDANCE_ALG == PHANTOM_LOCK
	this->tablewise_lock = (row_t *) _mm_malloc(sizeof(row_t), ALIGN_SIZE);
	new (this->tablewise_lock) row_t();
	this->tablewise_lock->init(this, 999, 0); // 999 for special mark
	this->tablewise_lock->init_manager(this->tablewise_lock);
	ex_lock_owner = 0;
#endif

}

void table_t::init(Catalog * schema, uint64_t part_cnt) {
	this->table_name = schema->table_name;
	this->schema = schema;
	this->part_cnt = part_cnt;
	_primary_index = NULL; 

#if TPCC_PHANTOM_AVOIDANCE && TPCC_PHANTOM_AVOIDANCE_ALG == PHANTOM_LOCK
	this->tablewise_lock = (row_t *) _mm_malloc(sizeof(row_t), ALIGN_SIZE);
	new (this->tablewise_lock) row_t();
	this->tablewise_lock->init(this, 999, 0); // 999 for special mark
	this->tablewise_lock->init_manager(this->tablewise_lock);
	ex_lock_owner = 0;
#endif

}


RC table_t::get_new_row(row_t *& row) {
	// this function is obsolete. 
	assert(false);
	return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {
	RC rc = RCOK;
	cur_tab_size ++;
	
	row = (row_t *) _mm_malloc(sizeof(row_t), ALIGN_SIZE);
	new (row) row_t();
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);
	return rc;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id, void * mem, void * manager_mem, void * data_mem) {
	RC rc = RCOK;
	cur_tab_size ++;
	
	row = (row_t *) mem;
	new (row) row_t();
	rc = row->init(this, part_id, row_id, data_mem);
	row->init_manager(row, manager_mem);
	return rc;
}
