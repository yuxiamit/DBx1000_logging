#pragma once 

#include "global.h"

class row_t;
class table_t;
class IndexHash;
class IndexMBTree;
class IndexArray;
class index_btree;
class Catalog;
class lock_man;
class txn_man;
class thread_t;
class index_base;
class Timestamp;
class Mvcc;

// this is the base class for all workload
class workload
{
public:
	// tables indexed by table name
	map<string, table_t *> tables;
	map<string, INDEX *> indexes; 
	// this is for YCSB
	map<string, HASH_INDEX *> hash_indexes;
	map<string, ARRAY_INDEX *> array_indexes;
	map<string, ORDERED_INDEX *> ordered_indexes;
	
	// initialize the tables and indexes.
	virtual RC init();
	virtual RC init_schema(string schema_file);
	virtual RC init_table()=0;
	virtual RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd)=0;
	
	volatile uint32_t sim_done;
public:
/*
	void index_insert(string index_name, uint64_t key, row_t * row);
	void index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id = -1);
	*/
template <class IndexT>
	void index_insert(IndexT* index, uint64_t key, row_t* row, int part_id);

};

template <class IndexT>
void workload::index_insert(IndexT* index, uint64_t key, row_t* row,
                            int part_id) {
  auto rc = index->index_insert(NULL, key, row, part_id);
  assert(rc == RCOK);
}