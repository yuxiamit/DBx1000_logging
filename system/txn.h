#pragma once 

#include "global.h"
#include "helper.h"
#include <unordered_map>

class workload;
class thread_t;
class row_t;
class table_t;
class base_query;
//class INDEX;
class HASH_INDEX;
class ARRAY_INDEX;
class ORDERED_INDEX;
class PredecessorInfo; 	

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_Blocked, VLL_Free};

class ScanHistory {
public:
	ORDERED_INDEX* idx;
	bool rev;
	uint64_t key;
	uint64_t max_key;
	uint64_t row_count; // assuming 2pl, we only need to record scan counts to prevent phantoms.
	uint64_t part_id;
};

class Access {
public:
	access_t 	type;
	row_t * 	orig_row;
	char * 		data;
	char * 		orig_data;

	void cleanup();
#if CC_ALG == TICTOC
	ts_t 		wts;
	ts_t 		rts;
#elif CC_ALG == SILO
	ts_t 		tid;
	ts_t 		epoch;
#elif CC_ALG == HEKATON
	void * 		history_entry;	
#endif

};

class txn_man
{
public:
	virtual void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
	void release();
	thread_t * h_thd;
	workload * h_wl;
	myrand * mrand;
	uint64_t abort_cnt;

	virtual RC 		run_txn(base_query * m_query, bool rec=false) = 0;
	uint64_t 		get_thd_id();
	workload * 		get_wl();
	void 			set_txn_id(txnid_t txn_id);
	txnid_t 		get_txn_id();

	void 			set_ts(ts_t timestamp);
	ts_t 			get_ts();

	pthread_mutex_t txn_lock;
	row_t * volatile cur_row;
#if CC_ALG == HEKATON
	void * volatile history_entry;
#endif
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	bool volatile 	lock_ready;
	bool volatile 	lock_abort; // forces another waiting txn to abort.
	// [TIMESTAMP, MVCC]
	bool volatile 	ts_ready; 
	// [HSTORE]


	int volatile 	ready_part;
	RC 				finish(RC rc);
	RC 				cleanup(RC rc);
#if CC_ALG == TICTOC
	uint64_t 		get_max_wts() 	{ return _max_wts; }
	void 			update_max_wts(ts_t max_wts);
	uint64_t 		get_commit_ts() { return _commit_ts; }
	uint64_t 		last_wts;
	uint64_t 		last_rts;
#elif CC_ALG == SILO
	ts_t 			last_tid;
#endif
	
	// For OCC
	uint64_t 		start_ts;
	uint64_t 		end_ts;
	// following are public for OCC
	//uint32_t		access_row_cnt;
	uint32_t 		row_cnt;
	uint32_t		wr_cnt;
	Access **		accesses;
	uint32_t * 		write_set;		// store indexes to accesses for writes 
	uint32_t 			num_accesses_alloc;

	// For VLL
	TxnType 		vll_txn_type;

	template <typename IndexT>
	itemid_t *		index_read(IndexT * index, idx_key_t key, int part_id);

	template <typename IndexT>
	RC index_read(IndexT* index, idx_key_t key, row_t** row, int part_id);

	template <typename IndexT>
	void index_read(IndexT * index, idx_key_t key, int part_id, itemid_t *& item);
	
	template <typename IndexT>
	RC index_read_multiple(IndexT* index, idx_key_t key, row_t** rows, size_t& count, int part_id);

	template <typename IndexT>
	row_t* search(IndexT* index, uint64_t key, int part_id,
                        access_t type, bool skip_read=false);
	
	template <typename IndexT>
	RC index_read_range(IndexT* index, idx_key_t min_key, idx_key_t max_key, row_t ** rows, size_t& count, int part_id);

	template <typename IndexT>
	RC index_read_range_rev(IndexT* index, idx_key_t min_key, idx_key_t max_key, row_t** rows, size_t& count, int part_id);

	RC apply_index_changes(RC rc);

	row_t * 		get_row(row_t * row, access_t type);
	
	RC				get_row(row_t * row, access_t type, char * &data);

	// For LOGGING
	void 			recover();
	// for parallel command logging, should recover by epoch. 
	uint64_t 		_last_epoch_time;

	// Stats
	void 			set_start_time(uint64_t time) { _txn_start_time = time; }
	uint64_t 		get_start_time() { return _txn_start_time; }

protected:	
	void 			insert_row(row_t * row, table_t * table);
	bool insert_row(table_t* tbl, row_t*& row, int part_id, uint64_t& out_row_id);
	bool remove_row(row_t* row);

	template <typename IndexT>
	bool insert_idx(IndexT* index, uint64_t key, row_t* row, int part_id, int index_id);

	template <typename IndexT>
	bool remove_idx(IndexT* index, uint64_t key, row_t* row, int part_id, int index_id);


//private:
	// insert rows
	// insert/remove rows
	uint64_t 		insert_cnt;
	row_t * 		insert_rows[MAX_ROW_PER_TXN];
	uint64_t 		remove_cnt;
	row_t * 		remove_rows[MAX_ROW_PER_TXN];
	uint64_t		scan_cnt;
	ScanHistory *	scan_results;
	
	//uint32_t find_index_id(ORDERED_INDEX* idx);

	// insert/remove indexes
	uint64_t 		   insert_idx_cnt;
	ORDERED_INDEX*   insert_idx_idx[MAX_ROW_PER_TXN];
	uint32_t		insert_idx_id[MAX_ROW_PER_TXN];
	idx_key_t	     insert_idx_key[MAX_ROW_PER_TXN];
	row_t* 		     insert_idx_row[MAX_ROW_PER_TXN];
	uint32_t		insert_idx_row_id[MAX_ROW_PER_TXN];
	int	       	   insert_idx_part_id[MAX_ROW_PER_TXN];

	uint64_t 		   remove_idx_cnt;
	ORDERED_INDEX*   remove_idx_idx[MAX_ROW_PER_TXN];
	int  		remove_idx_id[MAX_ROW_PER_TXN];
	idx_key_t	     remove_idx_key[MAX_ROW_PER_TXN];
	int	      	   remove_idx_part_id[MAX_ROW_PER_TXN];

        // node set for phantom avoidance
        std::unordered_map<void*, uint64_t>  node_map;
        friend class IndexHash;
        friend class IndexArray;
        friend class IndexMBTree;
        friend class IndexMBTree_cb;
        friend class IndexMICAMBTree;
        friend class IndexMICAMBTree_cb;

	uint64_t 		txn_id;
	ts_t 			timestamp;

#if CC_ALG == TICTOC || CC_ALG == SILO
	bool 			_write_copy_ptr;
	bool 			_pre_abort;
	bool 			_validation_no_wait;
#endif
#if CC_ALG == TICTOC
	bool			_atomic_timestamp;
	uint64_t		_max_wts;
	uint64_t		_commit_ts; 
	// the following methods are defined in concurrency_control/tictoc.cpp
	RC				validate_tictoc();
	uint64_t		_min_cts;
#elif CC_ALG == SILO
	uint64_t 		_epoch;
	
	RC				validate_silo();
	RC				validate_silo_serial();
#elif CC_ALG == HEKATON
	RC 				validate_hekaton(RC rc);
#endif

	// Stats
	uint64_t 		_txn_start_time;
	lsnType 			_cur_tid;

////////////////////////////////////////////////////
// Logging 
////////////////////////////////////////////////////
public:
#if LOG_ALGORITHM == LOG_PARALLEL
	void add_pred(uint64_t pred_tid, uint64_t key, uint32_t table, DepType type) {
		if (type == RAW) {
			_raw_preds_tid[_num_raw_preds] = pred_tid;
			#if TRACK_WAR_DEPENDENCY
			_raw_preds_key[_num_raw_preds] = key;
			_raw_preds_table[_num_raw_preds] = table;
			#endif
			_num_raw_preds++;
		} else if (type == WAW) {
			_waw_preds_tid[_num_waw_preds] = pred_tid;
			#if TRACK_WAR_DEPENDENCY
			_waw_preds_key[_num_waw_preds] = key;
			_waw_preds_table[_num_waw_preds] = table;
			#endif
			_num_waw_preds++;
		}
	};
#elif LOG_ALGORITHM == LOG_TAURUS
	uint32_t thread_local_counter;
#elif LOG_ALGORITHM == LOG_SERIAL
	void update_lsn(uint64_t lsn) {
		if (lsn > _max_lsn)
			_max_lsn = lsn;
	}
#endif
protected:	
	//virtual uint32_t get_cmd_log_size() { assert(false); }
	virtual void 	get_cmd_log_entry() { assert(false); }
	virtual uint32_t 	get_cmd_log_entry_length() { assert(false); return 0; }
	virtual void 	recover_txn(char * log_entry, uint64_t tid = (uint64_t)-1)  
	{ assert(false); }
	//RecoverState * recover_state) { assert(false); }
	uint32_t 		_log_entry_size;
	char * 			
	_log_entry;
public:
	void 			try_commit_txn();	
//private:
	uint32_t 		get_log_entry_size();
	// log entry stored in _log_entry_size, and _log_entry.
	void 			create_log_entry();
	uint32_t		get_log_entry_length();
#if LOG_ALGORITHM == LOG_PARALLEL
	uint32_t _num_raw_preds; 
	uint32_t _num_waw_preds; 
	uint64_t _raw_preds_tid[MAX_ROW_PER_TXN];
	uint64_t _waw_preds_tid[MAX_ROW_PER_TXN];
  #if TRACK_WAR_DEPENDENCY
	uint64_t _raw_preds_key[MAX_ROW_PER_TXN];
	uint64_t _waw_preds_key[MAX_ROW_PER_TXN];
	uint32_t _raw_preds_table[MAX_ROW_PER_TXN];
	uint32_t _waw_preds_table[MAX_ROW_PER_TXN];
  #endif
#elif LOG_ALGORITHM == LOG_SERIAL
	// For serial logging, we only maintain the max_lsn seen by the transaction.
	// this is an ELR technique from the following paper.
	// Kimura, Hideaki, Goetz Graefe, and Harumi A. Kuno. 
	// "Efficient locking techniques for databases on modern hardware." ADMS@ VLDB. 2012.
	lsnType _max_lsn;
#elif LOG_ALGORITHM == LOG_TAURUS
	lsnType _max_lsn;
	lsnType * lsn_vector;
#endif


	//void * _txn_node;
	struct TxnState {
		//public:
	#if LOG_ALGORITHM == LOG_SERIAL
		uint64_t max_lsn;	// the LSN
	#elif LOG_ALGORITHM == LOG_PARALLEL
		// preds is only used to determine when the transaction can commit. 
		// preds includes all predecessors, both RAW and WAW.
		// preds is stored in a compressed form, where only the max LSN is stored for each logger.
		uint64_t preds[NUM_LOGGER]; 
	#elif LOG_ALGORITHM == LOG_BATCH
		uint64_t epoch;
	#elif LOG_ALGORITHM == LOG_TAURUS
		uint64_t * lsn_vec;
		//uint64_t max_lsn;
		//uint32_t destination; // the target log file
		// empty for now
	#endif
		uint64_t start_time;
		uint64_t wait_start_time;
		/*TxnState(){
			cout << "constructor called" << endl;
			lsn_vec = (uint64_t*) _mm_malloc( sizeof(uint64_t) * g_num_logger, ALIGN_SIZE);
		}
		~TxnState(){
			_mm_free(lsn_vec);
		}*/
	};
	// Per-thread pending transactions waiting to commit 
	//queue<TxnState> ** _txn_state_queue;
	queue<TxnState> * _txn_state_queue;
	
#if LOG_ALGORITHM == LOG_TAURUS
	uint64_t * queue_lsn_vec_buffer;

	uint64_t queue_lsn_vec_buffer_length;
	uint64_t queue_lsn_vec_counter;
#endif

#if LOG_ALGORITHM == LOG_SERIAL
	void 			serial_recover();
#elif LOG_ALGORITHM == LOG_PARALLEL
	void 			parallel_recover();
#elif LOG_ALGORITHM == LOG_BATCH
	void 			batch_recover();
	static pthread_mutex_t * _log_lock;
#elif LOG_ALGORITHM == LOG_TAURUS
	void			taurus_recover();
	void			taurus_recover_high_contention();
#endif
public:
	uint64_t		last_writer;
//	uint64_t 		pred_vector[4];
//	uint64_t 		aggregate_pred_vector[4];
//	PredecessorInfo * getPredecessorInfo() { return _predecessor_info; }
//  #if LOG_TYPE == LOG_COMMAND
//	RecoverState * _recover_state;
//	RecoverState * get_recover_state() { return _recover_state; }
//  #endif
};
