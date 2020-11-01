#include "txn.h"
#include "row.h"
#include "row_silo.h"
#include "mem_alloc.h"
#include "table.h"
#include "locktable.h"

#if CC_ALG==SILO

void 
Row_silo::init(row_t * row) 
{
	_row = row;
#if ATOMIC_WORD
	_tid_word = 0;
#else 
	_latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), ALIGN_SIZE);
	pthread_mutex_init( _latch, NULL );
	_tid = 0;
#endif
}

RC
Row_silo::access(txn_man * txn, TsType type, char * data) {
#if LOG_ALGORITHM == LOG_SERIAL
	assert(ATOMIC_WORD);
	uint64_t v = _tid_word;
	while(v & LOCK_BIT)
	{
		PAUSE
		v = _tid_word;
	}
	uint64_t tid = v & LOCK_TID_MASK;
	txn->last_tid = tid;
 
  	//txn->update_lsn(tid); //???

#else
#if ATOMIC_WORD
  #if LOG_ALGORITHM == LOG_SERIAL || LOG_ALGORITHM == LOG_PARALLEL
	uint64_t pred; 
  #endif
	uint64_t v = 0;
	uint64_t v2 = 1;
	while (v2 != v) {

		v = _tid_word;
		while (v & LOCK_BIT) {
			PAUSE
			v = _tid_word;
		}
		//COMPILER_BARRIER
		memcpy(data, _row->get_data(), _row->get_tuple_size());
		//local_row->copy(_row);
  #if LOG_ALGORITHM == LOG_SERIAL || LOG_ALGORITHM == LOG_PARALLEL
		pred = _row->get_last_writer();
  #endif
		COMPILER_BARRIER
		v2 = _tid_word;
	}

	txn->last_tid = v & (~LOCK_BIT);
  #if LOG_ALGORITHM == LOG_PARALLEL
    //if (pred != (uint64_t)-1)
	txn->add_pred( pred, _row->get_primary_key(), _row->get_table()->get_table_id(),
		(type == R_REQ)? RAW : WAW);
	//if (_row->get_primary_key() == 1 && _row->get_table()->get_table_id() == 0)
	//	printf("last_writer = %ld\n", pred);
  #elif LOG_ALGORITHM == LOG_SERIAL
  	txn->update_lsn(pred);
  #endif
#else 
	lock();
	memcpy(data, _row->get_data(), _row->get_tuple_size());
	//local_row->copy(_row);
	txn->last_tid = _tid;
	release();
#endif
#endif
	return RCOK;
}

bool
Row_silo::validate(ts_t tid, bool in_write_set, lsnType * lsn_vec, bool update, uint64_t curtid) {
#if LOG_ALGORITHM == LOG_TAURUS
	if(lsn_vec != NULL)
	{
		
		for(uint32_t i=0; i<g_num_logger; ++i)
		{
			
			LockTableListItem * lti = (LockTableListItem*) _row->_lti_addr;
			if(lti != NULL)
			{
				uint64_t tempval = lti->readLV[i];
				while(tempval < lsn_vec[i] && !ATOM_CAS(lti->readLV[i], tempval, lsn_vec[i]))
				{
					if((lti = (LockTableListItem*) _row->_lti_addr) == NULL)
						break;
					tempval = lti->readLV[i];
				}
			} // if lti is NULL we don't need to update its readLV, as it will be refreshed later
		}
		 
	}
#endif
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	#if LOG_ALGORITHM == LOG_SERIAL
	return tid == (v & LOCK_TID_MASK);
	#else
	if (in_write_set)
		return tid == (v & (~LOCK_BIT));

	if (v & LOCK_BIT) 
		return false;
	else if(tid != v) //if (tid != (v & (~LOCK_BIT)))
		return false;
	else 
		{
			if(update) // curtid must be larger than tid
			{
				return ATOM_CAS(_tid_word, tid, curtid);
			}
			return true;
		}
	#endif
#else
	if (in_write_set)	
		return tid == _tid;
	if (!try_lock())
		return false;
	bool valid = (tid == _tid);
	release();
	return valid;
#endif
}

void
Row_silo::write(char * data, uint64_t tid) {
	_row->copy(data);
#if ATOMIC_WORD
	//uint64_t v = _tid_word;
	//M_ASSERT(tid >= (v & (~LOCK_BIT)) && (v & LOCK_BIT), "tid=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n", tid, (v & LOCK_BIT), (v & (~LOCK_BIT)));
  #if LOG_ALGORITHM != NO_LOG
    _row->set_last_writer(tid);
  #endif
  	COMPILER_BARRIER
	_tid_word = (tid | LOCK_BIT);
#else
	_tid = tid;
#endif
}

#if LOG_ALGORITHM == LOG_SERIAL
void Row_silo::lock(bool shared) {
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if(!shared)
	{
		while ((v & (~LOCK_TID_MASK)) || !__sync_bool_compare_and_swap(&_tid_word, v, v | LOCK_BIT)) {
			PAUSE
			v = _tid_word;
		}
	}
	else
	{
		while((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_tid_word, v, v + (1UL<<56))) {
			PAUSE
			v = _tid_word;
		}
	}
#else
	pthread_mutex_lock( _latch );
#endif
}
#else
void
Row_silo::lock() {
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_tid_word, v, v | LOCK_BIT)) {
		PAUSE
		v = _tid_word;
	}
#else
	pthread_mutex_lock( _latch );
#endif
}
#endif

#if LOG_ALGORITHM == LOG_SERIAL
void Row_silo::release(bool shared) {
#if ATOMIC_WORD
if(!shared)
{
	assert(_tid_word & LOCK_BIT);
	_tid_word = _tid_word & (~LOCK_BIT);
}
else
{
	assert(_tid_word & (~LOCK_TID_MASK));
	uint64_t v = _tid_word;
	while(!ATOM_CAS(_tid_word, v, v - (1UL << 56))) {
		PAUSE
		v = _tid_word;
	};
	//_tid_word = _tid_word - (1UL << 56);
}
#else 
	pthread_mutex_unlock( _latch );
#endif
}
#else
void
Row_silo::release() {
#if ATOMIC_WORD
	assert(_tid_word & LOCK_BIT);
	_tid_word = _tid_word & (~LOCK_BIT);
#else 
	pthread_mutex_unlock( _latch );
#endif
}
#endif

#if LOG_ALGORITHM  == LOG_SERIAL
bool
Row_silo::try_lock(bool shared)
{
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if(!shared)
	{
		if (v & (~LOCK_TID_MASK)) // already locked
			return false;
		return __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));
	}
	else
	{
		if(v & LOCK_BIT)
			return false;
		return __sync_bool_compare_and_swap(&_tid_word, v, v + (1UL << 56));
	}
#else
	return pthread_mutex_trylock( _latch ) != EBUSY;
#endif
}
#else
bool
Row_silo::try_lock()
{
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if (v & LOCK_BIT) // already locked
		return false;
	return __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));
#else
	return pthread_mutex_trylock( _latch ) != EBUSY;
#endif
}
#endif

uint64_t 
Row_silo::get_tid()
{
	assert(ATOMIC_WORD);
#if LOG_ALGORITHM == LOG_SERIAL
	return _tid_word & LOCK_TID_MASK;
#else
	return _tid_word & (~LOCK_BIT);
#endif
}

#endif
