#include "global.h"
#include "helper.h"
#include "wl.h"
#include "tpcc.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_array.h"
#include "index_mbtree.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpcc_const.h"
#include "manager.h"

RC tpcc_wl::init() {
	workload::init();
	next_tid = 0;
	string path = "./benchmarks/";
#if TPCC_SMALL
	path += "TPCC_short_schema.txt";
#else
	path += "TPCC_full_schema.txt";
#endif
	cout << "reading schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "TPCC schema initialized" << endl;
	init_table();
	
	return RCOK;
}

RC tpcc_wl::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	t_warehouse = tables["WAREHOUSE"];
	t_district = tables["DISTRICT"];
	t_customer = tables["CUSTOMER"];
	t_history = tables["HISTORY"];
	t_neworder = tables["NEW-ORDER"];
	t_order = tables["ORDER"];
	t_orderline = tables["ORDER-LINE"];
	t_item = tables["ITEM"];
	t_stock = tables["STOCK"];

	for (uint32_t i = 0; i < NUM_TABLES; i ++) {
		TableName num = (TableName) i;
		tpcc_tables[num] = tables[ table_num2str(num) ]; 
		if (tpcc_tables[num])
			tpcc_tables[num]->set_table_id(num);
	}

	i_item = hash_indexes["HASH_ITEM_IDX"];
	i_warehouse = hash_indexes["HASH_WAREHOUSE_IDX"];
	i_district = hash_indexes["HASH_DISTRICT_IDX"];
	i_customer_id = hash_indexes["HASH_CUSTOMER_ID_IDX"];
	i_customer_last = hash_indexes["HASH_CUSTOMER_LAST_IDX"];
	i_stock = hash_indexes["HASH_STOCK_IDX"];
	i_order = ordered_indexes["ORDERED_ORDER_IDX"];
	i_order_cust = ordered_indexes["ORDERED_ORDER_CUST_IDX"];
	i_neworder = ordered_indexes["ORDERED_NEWORDER_IDX"];
	i_orderline = ordered_indexes["ORDERED_ORDERLINE_IDX"];

	t_item->set_primary_index(i_item);
	t_warehouse->set_primary_index(i_warehouse);
	t_district->set_primary_index(i_district);
	t_customer->set_primary_index(i_customer_id);
	t_stock->set_primary_index(i_stock);

	t_order->set_primary_ordered_index(i_order);
	t_neworder->set_primary_ordered_index(i_neworder);
	t_orderline->set_primary_ordered_index(i_orderline);
	
	return RCOK;
}

void InitNURand(uint64_t thd_id) {
  C_255 = (uint64_t)URand(0, 255, thd_id);
  C_1023 = (uint64_t)URand(0, 1023, thd_id);
  C_8191 = (uint64_t)URand(0, 8191, thd_id);
}

RC tpcc_wl::init_table() {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order 
//		- new order
//		- order line
/**********************************/
	C_255 = (uint64_t) URand(0,255);
	C_1023 = (uint64_t) URand(0,1023);
	C_8191 = (uint64_t) URand(0,8191);
	int wh_thd_max = std::max(g_num_wh, g_thread_cnt);
  	tpcc_buffer = new drand48_data*[wh_thd_max];
	for (int i = 0; i < wh_thd_max; i++) {
		tpcc_buffer[i] =
			(drand48_data*)mem_allocator.alloc(sizeof(drand48_data), -1);
		srand48_r(i + 1, tpcc_buffer[i]);
  	}
  	InitNURand(0);

	pthread_t * p_thds = new pthread_t[g_num_wh - 1];
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
	threadInitWarehouse(this);
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_join(p_thds[i], NULL);

	printf("TPCC Data Initialization Complete!\n");
	return RCOK;
}

RC tpcc_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), ALIGN_SIZE);
	new(txn_manager) tpcc_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

void tpcc_wl::init_tab_item() {
	for (uint64_t i = 1; i <= g_max_items; i++) {
		row_t * row;
		uint64_t row_id;
		t_item->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(I_ID, i);
		row->set_value(I_IM_ID, URand(1L,10000L, 0));
		char name[24];
		MakeAlphaString(14, 24, name, 0);
		row->set_value(I_NAME, name);
		//row->set_value(I_PRICE, URand(1, 100, 0));
		row->set_value(I_PRICE, (double)URand(100L, 10000L, 0) / 100.0);
		char data[50];
    	MakeAlphaString(26, 50, data, 0);
		// TODO in TPCC, "original" should start at a random position
		if (RAND(10, 0) == 0) 
			strcpy(data, "original");		
		row->set_value(I_DATA, data);
		
		index_insert(i_item, i, row, 0);
	}
}

void tpcc_wl::init_tab_wh(uint32_t wid) {
	assert(wid >= 1 && wid <= g_num_wh);
	row_t * row;
	uint64_t row_id;
	t_warehouse->get_new_row(row, 0, row_id);
	row->set_primary_key(wid);

	row->set_value(W_ID, (uint64_t)wid);
	char name[10];
    MakeAlphaString(6, 10, name, wid-1);
	row->set_value(W_NAME, name);
	char street[20];
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_STREET_1, street);
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_STREET_2, street);
    MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_CITY, street);
	char state[2];
	MakeAlphaString(2, 2, state, wid-1); /* State */
	row->set_value(W_STATE, state);
	char zip[9];
   	MakeNumberString(9, 9, zip, wid-1); /* Zip */
	row->set_value(W_ZIP, zip);
   	double tax = (double)URand(0L,200L,wid-1)/1000.0;
   	double w_ytd=300000.00;
	row->set_value(W_TAX, tax);
	row->set_value(W_YTD, w_ytd);
	
	index_insert(i_warehouse, wid, row, wh_to_part(wid));
	return;
}

void tpcc_wl::init_tab_dist(uint64_t wid) {
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		row_t * row;
		uint64_t row_id;
		t_district->get_new_row(row, 0, row_id);

		uint64_t key = distKey(did, wid);
		row->set_primary_key(key);
		
		row->set_value(D_ID, did);
		row->set_value(D_W_ID, wid);
		char name[10];
		MakeAlphaString(6, 10, name, wid-1);
		row->set_value(D_NAME, name);
		char street[20];
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_CITY, street);
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(D_STATE, state);
		char zip[9];
    	MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(D_ZIP, zip);
    	double tax = (double)URand(0L,200L,wid-1)/1000.0;
    	double w_ytd=30000.00;
		row->set_value(D_TAX, tax);
		row->set_value(D_YTD, w_ytd);
		row->set_value(D_NEXT_O_ID, 3001);
		
		index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));
	}
}

void tpcc_wl::init_tab_stock(uint64_t wid) {
	
	for (uint64_t sid = 1; sid <= g_max_items; sid++) {
		row_t * row;
		uint64_t row_id;
		t_stock->get_new_row(row, 0, row_id);
		row->set_primary_key( stockKey(sid, wid) );
		row->set_value(S_I_ID, sid);
		row->set_value(S_W_ID, wid);
		row->set_value(S_QUANTITY, URand(10, 100, wid-1));
		row->set_value(S_REMOTE_CNT, 0);
#if !TPCC_SMALL
		char s_dist[25];
		char row_name[10] = "S_DIST_";
		for (int i = 1; i <= 10; i++) {
			if (i < 10) {
				row_name[7] = '0';
				row_name[8] = i + '0';
			} else {
				row_name[7] = '1';
				row_name[8] = '0';
			}
			row_name[9] = '\0';
			MakeAlphaString(24, 24, s_dist, wid-1);
			row->set_value(row_name, s_dist);
		}
		row->set_value(S_YTD, 0);
		row->set_value(S_ORDER_CNT, 0);
		char s_data[50];
		int len = MakeAlphaString(26, 50, s_data, wid-1);
		if (glob_manager->rand_uint64() % 100 < 10) {
			int idx = URand(0, len - 9, wid-1);
			strcpy(s_data +idx, "original");
		}
		row->set_value(S_DATA, s_data);
#endif
		index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));
	}
}

void tpcc_wl::init_tab_cust(uint64_t did, uint64_t wid) {
	assert(g_cust_per_dist >= 1000);
	for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) {
		row_t * row;
		uint64_t row_id;
		t_customer->get_new_row(row, 0, row_id);
		row->set_primary_key( custKey(cid, did, wid) );

		row->set_value(C_ID, cid);		
		row->set_value(C_D_ID, did);
		row->set_value(C_W_ID, wid);
		char c_last[LASTNAME_LEN];
		if (cid <= 1000)
			Lastname(cid - 1, c_last);
		else {
			Lastname(NURand(255,0,999,wid-1), c_last);
//			if (wid == 39 && cid > g_cust_per_dist - 10 && did == 4) 
//				cout << c_last << endl;
		}
		row->set_value(C_LAST, c_last);
#if !TPCC_SMALL
		char tmp[3] = "OE";
		row->set_value(C_MIDDLE, tmp);
		char c_first[FIRSTNAME_LEN];
		MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
		row->set_value(C_FIRST, c_first);
		char street[20];
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_STREET_1, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_STREET_2, street);
        MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_CITY, street); 
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(C_STATE, state);
		char zip[9];
    	MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(C_ZIP, zip);
		char phone[16];
  		MakeNumberString(16, 16, phone, wid-1); /* Zip */
		row->set_value(C_PHONE, phone);
		row->set_value(C_SINCE, (uint64_t)0);
		row->set_value(C_CREDIT_LIM, (double)50000);
		row->set_value(C_DELIVERY_CNT, (uint64_t)0);
		char c_data[500];
        MakeAlphaString(300, 500, c_data, wid-1);
		row->set_value(C_DATA, c_data);
#endif
		if (RAND(10, wid-1) == 0) {
			char tmp[] = "GC";
			row->set_value(C_CREDIT, tmp);
		} else {
			char tmp[] = "BC";
			row->set_value(C_CREDIT, tmp);
		}
		row->set_value(C_DISCOUNT, (double)RAND(5000,wid-1) / 10000);
		row->set_value(C_BALANCE, -10.0);
		row->set_value(C_YTD_PAYMENT, 10.0);
		row->set_value(C_PAYMENT_CNT, (uint64_t)1);
		row->set_value(C_DELIVERY_CNT, uint64_t(0));
		uint64_t key;
		key = custNPKey(c_last, did, wid);
		index_insert<HASH_INDEX>(i_customer_last, key, row, wh_to_part(wid));
		key = custKey(cid, did, wid);
		index_insert<HASH_INDEX>(i_customer_id, key, row, wh_to_part(wid));
	}
}

void tpcc_wl::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
	row_t * row;
	uint64_t row_id;
	t_history->get_new_row(row, 0, row_id);
	row->set_primary_key(0);
	row->set_value(H_C_ID, c_id);
	row->set_value(H_C_D_ID, d_id);
	row->set_value(H_D_ID, d_id);
	row->set_value(H_C_W_ID, w_id);
	row->set_value(H_W_ID, w_id);
	row->set_value(H_DATE, (uint64_t)0);
	row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
	char h_data[24];
	MakeAlphaString(12, 24, h_data, w_id-1);
	row->set_value(H_DATA, h_data);
#endif

}

void tpcc_wl::init_tab_order(uint64_t did, uint64_t wid) {
	uint64_t perm[g_cust_per_dist]; 
	init_permutation(perm, wid); /* initialize permutation of customer numbers */
	for (uint64_t oid = 1; oid <= g_cust_per_dist; oid++) {
		row_t * row;
		uint64_t row_id;
		t_order->get_new_row(row, 0, row_id);
		row->set_primary_key(orderKey(oid, did, wid));
		uint64_t o_ol_cnt = 1;
		uint64_t cid = oid; //perm[oid - 1]; //get_permutation();
		row->set_value(O_ID, oid);
		row->set_value(O_C_ID, cid);
		row->set_value(O_D_ID, did);
		row->set_value(O_W_ID, wid);
		uint64_t o_entry = 2013;
		row->set_value(O_ENTRY_D, o_entry);
		if (oid < 2101)
			row->set_value(O_CARRIER_ID, URand(1, 10, wid-1));
		else 
			row->set_value(O_CARRIER_ID, (uint64_t)0);
		o_ol_cnt = URand(5, 15, wid-1);
		row->set_value(O_OL_CNT, o_ol_cnt);
		row->set_value(O_ALL_LOCAL, (uint64_t)1);

#if TPCC_FULL
		index_insert(i_order, orderKey(oid, did, wid), row, wh_to_part(wid));
		index_insert(i_order_cust, orderCustKey(oid, cid, did, wid), row,
					wh_to_part(wid));
#endif
		
		// ORDER-LINE	
#if !TPCC_SMALL
		for (uint64_t ol = 1; ol <= o_ol_cnt; ol++) {
			t_orderline->get_new_row(row, 0, row_id);
			row->set_primary_key(orderlineKey(ol, oid, did, wid));
			row->set_value(OL_O_ID, oid);
			row->set_value(OL_D_ID, did);
			row->set_value(OL_W_ID, wid);
			row->set_value(OL_NUMBER, ol);
			row->set_value(OL_I_ID, URand(1, g_max_items, wid-1));
			row->set_value(OL_SUPPLY_W_ID, wid);
			if (oid < 2101) {
				row->set_value(OL_DELIVERY_D, o_entry);
				row->set_value(OL_AMOUNT, (uint64_t)0);
			} else {
				row->set_value(OL_DELIVERY_D, (uint64_t)0);
				row->set_value(OL_AMOUNT, (double)URand(1, 999999, wid-1)/100);
			}
			row->set_value(OL_QUANTITY, (uint64_t)5);
			char ol_dist_info[24];
	        MakeAlphaString(24, 24, ol_dist_info, wid-1);
			row->set_value(OL_DIST_INFO, ol_dist_info);
		}
#endif
		// NEW ORDER
		if (oid > 2100) {
			t_neworder->get_new_row(row, 0, row_id);
			row->set_primary_key(neworderKey(oid, did, wid));
			row->set_value(NO_O_ID, oid);
			row->set_value(NO_D_ID, did);
			row->set_value(NO_W_ID, wid);
			uint64_t n_oid;
			row->get_value(NO_O_ID, n_oid);
			assert(n_oid <= g_cust_per_dist);
#if TPCC_FULL
			index_insert(i_neworder, neworderKey(oid, did, wid), row,
						wh_to_part(wid));
#endif
		}
	}
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void 
tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
	uint64_t i;
	// Init with consecutive values
	for(i = 0; i < g_cust_per_dist; i++) 
		perm_c_id[i] = i+1;

	// shuffle
	for(i=0; i < g_cust_per_dist-1; i++) {
		uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
		uint64_t tmp = perm_c_id[i];
		perm_c_id[i] = perm_c_id[j];
		perm_c_id[j] = tmp;
	}
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

void * tpcc_wl::threadInitWarehouse(void * This) {
	tpcc_wl * wl = (tpcc_wl *) This;
	int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
	uint32_t wid = tid + 1;
	glob_manager->init_rand(tid);
	glob_manager->set_thd_id(tid);
	assert((uint64_t)tid < g_num_wh);

	if (tid == 0)
		wl->init_tab_item();
	wl->init_tab_wh( wid );
	wl->init_tab_dist( wid );
	wl->init_tab_stock( wid );
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		wl->init_tab_cust(did, wid);
		wl->init_tab_order(did, wid);
		for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) 
			wl->init_tab_hist(cid, did, wid);
	}
	return NULL;
}
