#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "tpcc.h"
#include "thread.h"
#include "logging_thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "log.h"
#include "serial_log.h"
#include "parallel_log.h"
#include "taurus_log.h"
#include "locktable.h"
#include "log_pending_table.h"
#include "log_recover_table.h"
#include "free_queue.h"
#include <execinfo.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

void * f(void *);
void * f_log(void *);

thread_t ** m_thds;
LoggingThread ** logging_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

void handler (int sig) {
	void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);

  raise (SIGABRT); // cause a core dump.
	//exit(1);
}

int main(int argc, char* argv[])
{
	//signal(SIGBUS, handler);   // install our handler
	uint64_t mainstart = get_sys_clock();
	if(BIG_HASH_TABLE_MODE==true)
		cout << "Running in big-hash-table mode." << endl;

	string dir;
	char hostname[256];
	gethostname(hostname, 256);
	if (strncmp(hostname, "draco", 5) == 0)
		dir = "./";
	if (strncmp(hostname, "yx", 2) == 0)
	{
		g_max_txns_per_thread = 100;
		cout << "[!] Detected desktop. Entering low disk-usage mode... " << endl;
	}
  	parser(argc, argv);
	cout << "Init parallelism " << g_init_parallelism << endl;
	if(g_thread_cnt < g_num_logger) g_num_logger = g_thread_cnt;

	stats = new Stats();
	stats->init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), ALIGN_SIZE);
	new(glob_manager) Manager();
	glob_manager->init();

	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
            assert(false);
			break;
		default:
			assert(false);
	}
	uint64_t init_start = get_sys_clock();
	m_wl->init();
	printf("workload initialized!\n");
	cout << "Init time " << (get_sys_clock() - init_start) / CPU_FREQ / 1e9 << endl;
	glob_manager->set_workload(m_wl);
	assert(GET_WORKLOAD->sim_done == 0);

	




#if LOG_ALGORITHM == LOG_TAURUS
	if(!g_log_recover) {
#if LOG_TYPE == LOG_DATA
		g_queue_buffer_length = g_log_buffer_size / 16;
#else
#if WORKLOAD == TPCC
		g_queue_buffer_length = g_log_buffer_size / 3; // empirical from Ln=8. TPCC Tm=0
#else
		g_queue_buffer_length = g_log_buffer_size;
#endif
	// this is a very conservative estimation assuming log items have no content at all.
#endif
		if(g_thread_cnt > 1) g_queue_buffer_length /= g_thread_cnt / 2; // we have a 2x coefficient here just for safe
		cout << "Queue Buffer Length" << g_queue_buffer_length <<  endl;
	}
#endif
	
	
	if(g_log_buffer_size % 512 != 0)
	{
		cout << "Bad log buffer size: " << g_log_buffer_size << endl;
		return 0;
	}

	cout << "Log Read Size: " << (uint64_t)(g_log_buffer_size * RECOVER_BUFFER_PERC) << endl;

#if LOG_ALGORITHM == LOG_SERIAL
	log_manager = new SerialLogManager;
	log_manager->init();
#elif LOG_ALGORITHM == LOG_TAURUS
	//string bench = (WORKLOAD == YCSB)? "YCSB" : "TPCC";
	log_manager = (TaurusLogManager*) _mm_malloc(sizeof(TaurusLogManager), 64); //new TaurusLogManager;
	new (log_manager) TaurusLogManager();
	log_manager->init();
#elif LOG_ALGORITHM == LOG_PARALLEL || LOG_ALGORITHM == LOG_BATCH
	string bench = "YCSB";
	if (WORKLOAD == TPCC)
	{
		bench = "TPCC_" + to_string(g_perc_payment);
	}
	log_manager = new LogManager * [g_num_logger];
	string type = (LOG_ALGORITHM == LOG_PARALLEL)? "P" : "B";
	if(LOG_ALGORITHM == LOG_TAURUS) type = "t";
	for (uint32_t i = 0; i < g_num_logger; i ++) {
		if (strncmp(hostname, "ip-", 3) == 0) { // EC2
			dir = "/data";
			dir += to_string(i % g_num_disk);
			dir += "/";			
		} 
		log_manager[i] = (LogManager *) _mm_malloc(sizeof(LogManager), ALIGN_SIZE);
		new(log_manager[i]) LogManager(i);
		#if LOG_TYPE == LOG_DATA
		log_manager[i]->init(dir + type + "D_log" + to_string(i) + "_" + to_string(g_num_logger) + "_" + bench + ".log");
		#else
		log_manager[i]->init(dir + type + "C_log" + to_string(i) + "_" + to_string(g_num_logger) + "_" + bench + ".log");
		#endif
	}
	
  #if LOG_ALGORITHM == LOG_PARALLEL
	if (g_log_recover) 
		MALLOC_CONSTRUCTOR(LogRecoverTable, log_recover_table);
  #endif
#endif
	next_log_file_epoch = new uint32_t * [g_num_logger];
	for (uint32_t i = 0; i < g_num_logger; i ++) {
		next_log_file_epoch[i] = (uint32_t *) _mm_malloc(sizeof(uint32_t), ALIGN_SIZE);
	}
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
#if USE_LOCKTABLE
	LockTable::getInstance();  // initialize the lock table singleton
	//LockTable::printLockTable();
#endif
	
#if CC_ALG == DL_DETECT
		dl_detector.init();
#endif
	printf("mem_allocator initialized!\n");
	

	uint64_t thd_cnt = g_thread_cnt;
	pthread_t * p_thds = (pthread_t*) _mm_malloc(sizeof(pthread_t) * (thd_cnt - 1), ALIGN_SIZE);
	pthread_t * p_logs = (pthread_t*) _mm_malloc(sizeof(pthread_t) * (g_num_logger), ALIGN_SIZE);;

	m_thds = (thread_t**) _mm_malloc(sizeof(thread_t*) * thd_cnt, ALIGN_SIZE); // new thread_t * [thd_cnt];
	logging_thds = (LoggingThread **) _mm_malloc(sizeof(LoggingThread*) * g_num_logger, ALIGN_SIZE);

	for (uint32_t i = 0; i < thd_cnt; i++) 
	{
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), ALIGN_SIZE);
		new(m_thds[i]) thread_t();
	}
	for (uint32_t i = 0; i < g_num_logger; i++)  
	{
		logging_thds[i] = (LoggingThread *) _mm_malloc(sizeof(LoggingThread), ALIGN_SIZE);
		new(logging_thds[i]) LoggingThread();
	}
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	if (!g_log_recover) {
		query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), ALIGN_SIZE);
		query_queue->init(m_wl);
		printf("query_queue initialized!\n");
	}
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	pthread_barrier_init( &worker_bar, NULL, g_thread_cnt );
#if LOG_ALGORITHM == LOG_NO // || LOG_ALGORITHM == LOG_BATCH
	pthread_barrier_init( &log_bar, NULL, g_thread_cnt );
//#elif LOG_ALGORITHM == LOG_BATCH
//	pthread_barrier_init( &log_bar, NULL, g_num_logger);
#else
	pthread_barrier_init( &log_bar, NULL, g_num_logger + g_thread_cnt );
#endif
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++) { 
		m_thds[i]->init(i, m_wl);
	}
#if LOG_ALGORITHM != LOG_NO
	for (uint32_t i = 0; i < g_num_logger; i++)
		logging_thds[i]->set_thd_id(i);
#endif

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt);

	// spawn and run txns again.
	int64_t starttime = get_server_clock();
	if(g_log_recover)
	{
		// change the order of threads.
		assert (LOG_ALGORITHM != LOG_NO);
		for (uint32_t i = 0; i < g_num_logger; i++) {
			uint64_t vid = i;
			pthread_create(&p_logs[i], NULL, f_log, (void *)vid);
		}
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
	}
	else
	{
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		if (LOG_ALGORITHM != LOG_NO) // && !g_log_recover)
			for (uint32_t i = 0; i < g_num_logger; i++) {
				uint64_t vid = i;
				pthread_create(&p_logs[i], NULL, f_log, (void *)vid);
			}
	}
	f((void *)(thd_cnt - 1));
	
	for (uint32_t i = 0; i < thd_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);
	if (LOG_ALGORITHM != LOG_NO) // && !g_log_recover)
		for (uint32_t i = 0; i < g_num_logger; i++) 
			pthread_join(p_logs[i], NULL);
	int64_t endtime = get_server_clock();
	cout << "PASS! SimTime = " << (endtime - starttime) / CPU_FREQ << endl;
	if (STATS_ENABLE) {
		stats->print();
	}
#if LOG_ALGORITHM == LOG_PARALLEL
	if (g_log_recover)
		log_recover_table->check_all_recovered();
#endif
		cout << "Total time measured " << float(get_sys_clock() - mainstart) / 1e9 << endl; // for CPU_FREQ calibration
    return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}
void * f_log(void * id) {
#if LOG_ALGORITHM != LOG_NO
	uint64_t tid = (uint64_t)id;
	logging_thds[tid]->run();
#endif
	return NULL;
}
