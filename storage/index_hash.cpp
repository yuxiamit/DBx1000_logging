#include "global.h"	
#include "index_hash.h"
#include "mem_alloc.h"
#include "table.h"
#include "manager.h"

RC IndexHash::init(uint64_t bucket_cnt, int part_cnt) {
	_bucket_cnt = bucket_cnt;
	_bucket_cnt_per_part = bucket_cnt / part_cnt;
	_buckets = new BucketHeader * [part_cnt];
	for (int i = 0; i < part_cnt; i++) {
		_buckets[i] = (BucketHeader *) _mm_malloc(sizeof(BucketHeader) * _bucket_cnt_per_part, ALIGN_SIZE);
		for (uint32_t n = 0; n < _bucket_cnt_per_part; n ++)
			_buckets[i][n].init();
	}
	return RCOK;
}

RC 
IndexHash::init(int part_cnt, table_t * table, uint64_t bucket_cnt) {
	init(bucket_cnt, part_cnt);
	this->table = table;
	return RCOK;
}

bool IndexHash::index_exist(idx_key_t key) {
	assert(false);
}

void 
IndexHash::get_latch(BucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void 
IndexHash::release_latch(BucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}

RC IndexHash::index_insert(IndexHash* index, uint64_t key, row_t* row, int part_id)
{
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;
    return index_insert(key, m_item, pid);
}

RC IndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id, void * node_mem) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	// 1. get the ex latch
	get_latch(cur_bkt);
	
	// 2. update the latch list
	cur_bkt->insert_item(key, item, part_id, node_mem);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}
	
RC IndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	// 1. get the ex latch
	get_latch(cur_bkt);
	
	// 2. update the latch list
	cur_bkt->insert_item(key, item, part_id);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;

}

RC IndexHash::index_read(txn_man* txn, idx_key_t key, row_t** row, int part_id) {
  uint64_t bkt_idx = hash(key);
  assert(bkt_idx < _bucket_cnt_per_part);
  BucketHeader* cur_bkt = &_buckets[part_id][bkt_idx];
  RC rc = RCOK;
  // 1. get the sh latch
  //	get_latch(cur_bkt);
  itemid_t* m_item;
  cur_bkt->read_item(key, m_item, table->get_table_name());
  if (m_item == NULL) return ERROR;
  *row = (row_t*)m_item->location;
  // 3. release the latch
  //	release_latch(cur_bkt);
  return rc;
}


RC IndexHash::index_read(idx_key_t key, itemid_t * &item, 
						int part_id, int thd_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;
}

RC IndexHash::index_read_multiple(txn_man* txn, idx_key_t key, row_t** rows, size_t& count,
                         int part_id) {
  uint64_t bkt_idx = hash(key);
  assert(bkt_idx < _bucket_cnt_per_part);
  BucketHeader* cur_bkt = &_buckets[part_id][bkt_idx];
  RC rc = RCOK;
  // 1. get the sh latch
  //	get_latch(cur_bkt);
  itemid_t* m_item;
  cur_bkt->read_item(key, m_item, table->get_table_name());
  size_t i = 0;
  while (m_item != NULL && i < count) {
    rows[i++] = (row_t*)m_item->location;
    m_item = m_item->next;
  }
  count = i;
  // 3. release the latch
  //	release_latch(cur_bkt);
  return rc;
}

/************** BucketHeader Operations ******************/

void BucketHeader::init() {
	node_cnt = 0;
	first_node = NULL;
	locked = false;
}


void BucketHeader::insert_item(idx_key_t key, 
		itemid_t * item, 
		int part_id, void * node_mem) 
{
	BucketNode * cur_node = first_node;
	BucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}
	if (cur_node == NULL) {		
		//BucketNode * new_node = (BucketNode *) _mm_malloc(sizeof(BucketNode), ALIGN_SIZE);
		BucketNode * new_node = (BucketNode *) node_mem;
		new_node->init(key);
		new_node->items = item;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
		item->next = cur_node->items;
		cur_node->items = item;
	}
}

void BucketHeader::insert_item(idx_key_t key, 
		itemid_t * item, 
		int part_id) 
{
	BucketNode * cur_node = first_node;
	BucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}
	if (cur_node == NULL) {		
		//BucketNode * new_node = (BucketNode *) _mm_malloc(sizeof(BucketNode), ALIGN_SIZE);
		BucketNode * new_node = (BucketNode *) mem_allocator.alloc(sizeof(BucketNode), part_id );
		new_node->init(key);
		new_node->items = item;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
		item->next = cur_node->items;
		cur_node->items = item;
	}
}

void BucketHeader::read_item(idx_key_t key, itemid_t * &item, const char * tname) 
{
	BucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		cur_node = cur_node->next;
	}
	M_ASSERT(cur_node->key == key, "Key does not exist!");
	item = cur_node->items;
}
