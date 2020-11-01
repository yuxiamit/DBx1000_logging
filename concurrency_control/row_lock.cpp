#include "row.h"
#include "txn.h"
#include "helper.h"
#include "row_lock.h"
#include "mem_alloc.h"
#include "manager.h"

#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT

#define CONFLICT(a, b) (a != LOCK_NONE_T && b != LOCK_NONE_T) && (a==LOCK_EX_T || b==LOCK_EX_T)

void Row_lock::init(row_t * row) {
	_row = row;
	/*
	owners = NULL;
	waiters_head = NULL;
	waiters_tail = NULL;
	*/
	// owner_cnt = 0;
	//waiter_cnt = 0;

	
#if !USE_LOCKTABLE
	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
	blatch = false;
#endif
	
	lock_type = LOCK_NONE_T;
	ownerCounter = 0;

}

RC Row_lock::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_lock::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	uint64_t starttime = get_sys_clock();
	assert (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE);
	RC rc;
	//int part_id =_row->get_part_id();
#if !USE_LOCKTABLE  // otherwise we don't need a latch here.
	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );
#endif
	
	// IMPORTANT: for simplicity, 
	// we assume that if a transaction reads and writes to the same tuple
	// it will first acquired the exclusive lock.
	bool conflict = CONFLICT(lock_type, type);// conflict_lock(lock_type, type);
	if (conflict) { 
		// Cannot be added to the owner list.
		if (CC_ALG == NO_WAIT) {
			rc = Abort;
		}
	} else {
		INC_INT_STATS(time_debug6, get_sys_clock() - starttime);
		//LockEntry entry = LockEntry {type, txn}; 
		//++ owner_cnt;
		//_owners.push_back(entry);
		ownerCounter ++;
		//printf("[%" PRIu64 "] Pushed %" PRIu64 " with type %d, current locktype %d\n", (uint64_t) _row, (uint64_t) txn, type, lock_type);
		lock_type = type;
        rc = RCOK;
	}
#if !USE_LOCKTABLE  // otherwise we don't need a latch here.
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );
#endif
	INC_INT_STATS(time_debug7, get_sys_clock() - starttime);
	return rc;
}


RC Row_lock::lock_release(txn_man * txn) {	

#if !USE_LOCKTABLE  // otherwise we don't need a latch here.
	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );
#endif

	ownerCounter --;
#if (CC_ALG == NO_WAIT)
	//assert(found);
#endif
	
	if (ownerCounter == 0)//(_owners.empty())
		lock_type = LOCK_NONE_T;

#if !USE_LOCKTABLE  // otherwise we don't need a latch here.
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );
#endif
	return RCOK;
}

bool Row_lock::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE_T || l2 == LOCK_NONE_T)
	{
		return false;
	}
  else if (l1 == LOCK_EX_T || l2 == LOCK_EX_T)
	{
        return true;
	}
	else
	{
		return false;
	}
}

LockEntry * Row_lock::get_entry() {
	LockEntry * entry = (LockEntry *) 
		mem_allocator.alloc(sizeof(LockEntry), _row->get_part_id());
	return entry;
}
void Row_lock::return_entry(LockEntry * entry) {
	mem_allocator.free(entry, sizeof(LockEntry));
}

#endif
