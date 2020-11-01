#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_mbtree.h"
#include "index_array.h"
#include "index_btree.h"
#include "tpcc_const.h"
#include "manager.h"
#include "row_silo.h"
#include <inttypes.h>

void tpcc_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpcc_wl *) h_wl;

#if TPCC_DBX1000_SERIAL_DELIVERY
  memset(active_delivery, 0, sizeof(active_delivery));
#endif
}

RC tpcc_txn_man::run_txn(base_query * query, bool rec) {
	_query = (tpcc_query *) query;
	RC rc = RCOK;
//	cout << _query->type << endl;
	uint64_t starttime = get_sys_clock();
	switch (_query->type) {
		case TPCC_PAYMENT :
			rc = run_payment(_query);
			//COMPILER_BARRIER 
			if(rc==RCOK)
				INC_INT_STATS(int_tpcc_payment_commit, 1);
			INC_INT_STATS(time_tpcc_payment, get_sys_clock() - starttime);
			break;
		case TPCC_NEW_ORDER :
			rc = run_new_order(_query); 
			//COMPILER_BARRIER
			if(rc==RCOK)
				INC_INT_STATS(int_tpcc_new_order_commit, 1);
			INC_INT_STATS(time_tpcc_new_order, get_sys_clock() - starttime);
			break;
		case TPCC_ORDER_STATUS :
			//assert(false);
			rc = run_order_status(_query);
			if(rc==RCOK)
			{
				INC_INT_STATS(int_tpcc_order_status_commit, 1);
			}
			else
			{
				INC_INT_STATS(int_tpcc_order_status_commit, 1);
			}
			
			INC_INT_STATS(time_tpcc_order_status, get_sys_clock() - starttime);
			break;
		case TPCC_DELIVERY :
			//assert(false);
			rc = run_delivery(_query);
			if(rc==RCOK)
			{
				INC_INT_STATS(int_tpcc_delivery_commit, 1);
			}
			else
			{
				INC_INT_STATS(int_delivery_aborts, 1);
			}
			
			INC_INT_STATS(time_tpcc_delivery, get_sys_clock() - starttime);
			break;
		case TPCC_STOCK_LEVEL :
			//assert(false);
			rc = run_stock_level(_query);
			if(rc==RCOK)
			{
				INC_INT_STATS(int_tpcc_stock_level_commit, 1);
			}
			else
			{
				INC_INT_STATS(int_stock_level_aborts, 1);
			}
			
			INC_INT_STATS(time_tpcc_stock_level, get_sys_clock() - starttime);
			break;
		default:
		
			M_ASSERT(false, "type=%d num_commit=%" PRIu64 "\n", 
				_query->type, stats->_stats[GET_THD_ID]->_int_stats[STAT_num_commits]);
	}
	return rc;
}

RC tpcc_txn_man::run_payment(tpcc_query * query) {
	RC rc = RCOK;
	uint64_t key;
	itemid_t * item;

	uint64_t w_id = query->w_id;
    uint64_t c_w_id = query->c_w_id;
#if VERBOSE_LEVEL & VERBOSE_SQL_CONTENT
    stringstream ss;
    ss << GET_THD_ID << "EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount	WHERE w_id=:w_id;" << w_id << endl;
    cout << ss.str();
	/*====================================================+ \
    	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount \
		WHERE w_id=:w_id; \
	+====================================================*/ \
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/
#endif
	// TODO for variable length variable (string). Should store the size of 
	// the variable.
	key = query->w_id;
	INDEX * index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);

	char * wh_data;
	if (g_wh_update)
		rc = get_row(r_wh, WR, wh_data);
	else 
		rc = get_row(r_wh, RD, wh_data);
	
	if (rc == Abort) {
		return finish(Abort);
	}
	double w_ytd = *(double *)row_t::get_value(r_wh->get_schema(), W_YTD, wh_data);
	if (g_wh_update) {
		double amount = w_ytd + query->h_amount;
		row_t::set_value(r_wh->get_schema(), W_YTD, wh_data, (char *)&amount);
	}
	char w_name[11];
	char * tmp_str = row_t::get_value(r_wh->get_schema(), W_NAME, wh_data);
	memcpy(w_name, tmp_str, 10);
	w_name[10] = '\0';

#if VERBOSE_LEVEL & VERBOSE_SQL_CONTENT
    stringstream ss2;
    ss2 << GET_THD_ID << "EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount WHERE d_w_id=:w_id AND d_id=:d_id;"<< w_id << " " << query->d_id << endl;
    cout << ss2.str();
#endif
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	// TODO TODO TODO
	key = distKey(query->d_id, query->d_w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	char * r_dist_data = NULL;
    rc = get_row(r_dist, WR, r_dist_data);
	if (rc != RCOK) {
		return finish(Abort);
	}

	//double d_ytd;
	double d_ytd = *(double *)row_t::get_value(r_dist->get_schema(), D_YTD, r_dist_data);
    d_ytd += query->h_amount;
    row_t::set_value(r_dist->get_schema(), D_YTD, r_dist_data, (char *)&d_ytd);
	//r_dist_local->get_value(D_YTD, d_ytd);
	//r_dist_local->set_value(D_YTD, d_ytd + query->h_amount);
	char d_name[11];
	tmp_str = row_t::get_value(r_dist->get_schema(), D_NAME, r_dist_data);
	memcpy(d_name, tmp_str, 10);
	d_name[10] = '\0';

	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/

	row_t * r_cust;
	if (query->by_last_name) { 
		/*==========================================================+
			EXEC SQL SELECT count(c_id) INTO :namecnt
			FROM customer
			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
		+==========================================================*/
		/*==========================================================================+
			EXEC SQL DECLARE c_byname CURSOR FOR
			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
			ORDER BY c_first;
			EXEC SQL OPEN c_byname;
		+===========================================================================*/
#if VERBOSE_LEVEL & VERBOSE_SQL_CONTENT
    stringstream ss3;
    ss3 << GET_THD_ID << "EXEC SQL SELECT count(c_id) INTO :namecnt FROM customer WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;"
    << query->c_last << " " << query->c_d_id << " " << query->c_w_id <<  endl;
    cout << ss3.str();
#endif
		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		HASH_INDEX * index = _wl->i_customer_last;

		row_t* rows[100];
  		size_t count = 100;

		auto rc = index_read_multiple(index, key, rows, count, wh_to_part(query->c_w_id));
		if (rc != RCOK) {
    		assert(false);
		}
		assert(count != 100);
		r_cust = rows[count / 2];

		/*
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);*/
		
		/*============================================================================+
			for (n=0; n<namecnt/2; n++) {
				EXEC SQL FETCH c_byname
				INTO :c_first, :c_middle, :c_id,
					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
			}
			EXEC SQL CLOSE c_byname;
		+=============================================================================*/
		// XXX: we don't retrieve all the info, just the tuple we are interested in
	}
	else { // search customers by cust_id
		/*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/
		key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		r_cust = (row_t *) item->location;
	}
#if VERBOSE_LEVEL & VERBOSE_SQL_CONTENT
    stringstream ss4;
    ss4 << GET_THD_ID << "EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;"
    << query->c_w_id << " " << query->c_d_id << " " << query->c_id <<  endl;
    cout << ss4.str();
#endif
  	/*======================================================================+
	   	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
   		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
   	+======================================================================*/
	char * r_cust_data = NULL;
    rc = get_row(r_cust, WR, r_cust_data);
	if (rc != RCOK) {
		return finish(Abort);
	}
	double c_balance = *(double *)row_t::get_value(r_cust->get_schema(), C_BALANCE, r_cust_data);
    c_balance -= query->h_amount;
    row_t::set_value(r_cust->get_schema(), C_BALANCE, r_cust_data, (char *)&c_balance);

    double c_ytd_payment = *(double *)row_t::get_value(r_cust->get_schema(), C_YTD_PAYMENT, r_cust_data);
    c_ytd_payment -= query->h_amount;
    row_t::set_value(r_cust->get_schema(), C_YTD_PAYMENT, r_cust_data, (char *)&c_ytd_payment);

    double c_payment_cnt = *(double *)row_t::get_value(r_cust->get_schema(), C_PAYMENT_CNT, r_cust_data);
    c_payment_cnt += 1;
    row_t::set_value(r_cust->get_schema(), C_PAYMENT_CNT, r_cust_data, (char *)&c_payment_cnt);

#if TPCC_FULL
	char * c_credit = row_t::get_value(r_cust->get_schema(), C_CREDIT, r_cust_data);
	if ( strstr(c_credit, "BC") ) {

		/*=====================================================+
		    EXEC SQL SELECT c_data
			INTO :c_data
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+=====================================================*/
	  	char c_new_data[501];
	  	sprintf(c_new_data,"| %4lu %2lu %4lu %2lu %4lu $%7.2f",
	      	query->c_id, query->c_d_id, query->c_w_id, query->d_id, w_id, query->h_amount);
		char cdata_colname[] = "C_DATA";
		char * c_data = r_cust->get_value(cdata_colname);
	  	strncat(c_new_data, c_data, 500 - strlen(c_new_data));
		r_cust->set_value(cdata_colname, c_new_data);			
	}

	char h_data[25];
	strncpy(h_data, w_name, 11);
	int length = strlen(h_data);
	if (length > 10) length = 10;
	strcpy(&h_data[length], "    ");
	strncpy(&h_data[length + 4], d_name, 11);
	h_data[length+14] = '\0';
	/*=============================================================================+
	  EXEC SQL INSERT INTO
	  history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
	  VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
	  +=============================================================================*/
	row_t * r_hist;
	uint64_t row_id;
	auto ret = insert_row(_wl->t_history, r_hist, wh_to_part(query->w_id), row_id);
	if(!ret) return finish(Abort);

	//_wl->t_history->get_new_row(r_hist, 0, row_id);
	r_hist->set_primary_key(0);
	r_hist->set_value(H_C_ID, query->c_id);
	r_hist->set_value(H_C_D_ID, query->c_d_id);
	r_hist->set_value(H_C_W_ID, query->c_w_id);
	r_hist->set_value(H_D_ID, query->d_id);
	r_hist->set_value(H_W_ID, query->w_id);
	int64_t date = 2013;		
	r_hist->set_value(H_DATE, date);
	r_hist->set_value(H_AMOUNT, query->h_amount);
#if !TPCC_SMALL
	r_hist->set_value(H_DATA, h_data);
#endif
	// We don't need to update histories.
#endif
	assert( rc == RCOK );
	if (g_log_recover)
		return RCOK;
	else 
		return finish(rc);
}

RC tpcc_txn_man::run_new_order(tpcc_query * query) {
	uint64_t starttime = get_sys_clock();
	RC rc = RCOK;
	uint64_t key;
	itemid_t * item;
	INDEX * index;
	
	bool remote = query->remote;
	uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
	uint64_t ol_cnt = query->ol_cnt;
	uint64_t ol_delivery_d = query->ol_delivery_d;
	uint64_t part_id = wh_to_part(w_id);
	/*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/
	key = w_id;
	index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	char * r_wh_data = NULL;
    rc = get_row(r_wh, RD, r_wh_data); //row_t * r_wh_local = get_row(r_wh, RD);
	if (rc != RCOK)
		return finish(Abort);

	//double w_tax;
	//r_wh_local->get_value(W_TAX, w_tax); 
	
	key = custKey(c_id, d_id, w_id);
	index = _wl->i_customer_id;
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_cust = (row_t *) item->location;

	char * r_cust_data = NULL;
    rc = get_row(r_cust, RD, r_cust_data);
	if (rc != RCOK)
		return finish(Abort);
	
	//uint64_t c_discount;
	//char * c_last;
	//char * c_credit;
	//r_cust_local->get_value(C_DISCOUNT, c_discount);
	//c_last = r_cust_local->get_value(C_LAST);
	//c_credit = r_cust_local->get_value(C_CREDIT);
 	
	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
	+===================================================*/
	key = distKey(d_id, w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
    
	char * r_dist_data = NULL;
    rc = get_row(r_dist, WR, r_dist_data);
    if (rc != RCOK)
        return finish(Abort);


    int64_t o_id = *(int64_t *) row_t::get_value(r_dist->get_schema(), D_NEXT_O_ID, r_dist_data);
    o_id ++;
    row_t::set_value(r_dist->get_schema(), D_NEXT_O_ID, r_dist_data, (char *)&o_id);	
	uint64_t tt1 = get_sys_clock();
	INC_INT_STATS(time_phase1_1, tt1 - starttime);
	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/
#if TPCC_FULL
	row_t * r_order;
	uint64_t row_id;
	//_wl->t_order->get_new_row(r_order, 0, row_id);
	if(!insert_row(_wl->t_order, r_order, part_id, row_id))
	{
		return finish(Abort);
	}
	r_order->set_primary_key(orderKey(o_id, d_id, w_id));
	r_order->set_value(O_ID, o_id);
	//assert(c_id <= g_cust_per_dist);
	r_order->set_value(O_C_ID, c_id);
	r_order->set_value(O_D_ID, d_id);
	r_order->set_value(O_W_ID, w_id);
	r_order->set_value(O_ENTRY_D, query->o_entry_d);
	r_order->set_value(O_CARRIER_ID, query->o_carrier_id);
	r_order->set_value(O_OL_CNT, ol_cnt);
	int64_t all_local = (remote? 0 : 1);
	r_order->set_value(O_ALL_LOCAL, all_local);
	/*if(g_log_recover)
	{
		printf("insert row primary_key %lu, c_id %lu\n", orderKey(o_id, d_id, w_id), c_id);
	}
	*/

#if TPCC_INSERT_INDEX
  // printf("new Order o_id=%" PRId64 "\n", o_id);
  {
    auto idx = _wl->i_order;
    auto key = orderKey(o_id, d_id, w_id);
    if (!insert_idx(idx, key, r_order, part_id, tpcc_wl::IID_ORDER)) return finish(Abort);
  }
  {
    auto idx = _wl->i_order_cust;
    auto key = orderCustKey(o_id, c_id, d_id, w_id);
    if (!insert_idx(idx, key, r_order, part_id, tpcc_wl::IID_ORDER_CUST)) return finish(Abort);
  }
#endif
	//insert_row(r_order, _wl->t_order);
	/*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
	row_t * r_no;
	if(!insert_row(_wl->t_neworder, r_no, part_id, row_id))
	{
		return finish(Abort);
	}
	
	//_wl->t_neworder->get_new_row(r_no, 0, row_id);
	r_no->set_primary_key(neworderKey(o_id, d_id, w_id));
	r_no->set_value(NO_O_ID, o_id);
	r_no->set_value(NO_D_ID, d_id);
	r_no->set_value(NO_W_ID, w_id);
	//insert_row(r_no, _wl->t_neworder);
#if TPCC_INSERT_INDEX
	// printf("new NewOrder o_id=%" PRId64 "\n", o_id);
	{
		auto idx = _wl->i_neworder;
		auto key = neworderKey(o_id, d_id, w_id);
		if (!insert_idx(idx, key, r_no, part_id, tpcc_wl::IID_NEWORDER)) return finish(Abort);
	}
#endif
#endif

	for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {
		uint64_t tt_i = get_sys_clock();
		uint64_t ol_i_id = query->items[ol_number].ol_i_id;
		uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
		uint64_t ol_quantity = query->items[ol_number].ol_quantity;
		/*===========================================+
		EXEC SQL SELECT i_price, i_name , i_data
			INTO :i_price, :i_name, :i_data
			FROM item
			WHERE i_id = :ol_i_id;
		+===========================================*/
		key = ol_i_id;
		item = index_read(_wl->i_item, key, 0); //<<<<<<<<<<<<<<<
		uint64_t tt_2 = get_sys_clock();
		INC_INT_STATS(time_phase1_2, tt_2 - tt_i);
		COMPILER_BARRIER
		assert(item != NULL);
		row_t * r_item = ((row_t *)item->location);
		
		char * r_item_data = NULL;
        rc = get_row(r_item, RD, r_item_data); //<<<<<<<<<<<<<<<<
		uint64_t tt_3 = get_sys_clock();
		INC_INT_STATS(time_phase2, tt_3 - tt_2);
		COMPILER_BARRIER
        if (rc != RCOK)
			return finish(Abort);

#if TPCC_FULL		
		int64_t i_price;
		//char * i_name;
		//char * i_data;
		r_item->get_value(I_PRICE, i_price);
		//i_name = r_item->get_value(I_NAME);
		//i_data = r_item->get_value(I_DATA);
#endif

		/*===================================================================+
		EXEC SQL SELECT s_quantity, s_data,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data,
				:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
				:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
			FROM stock
			WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
		EXEC SQL UPDATE stock SET s_quantity = :s_quantity
			WHERE s_i_id = :ol_i_id
			AND s_w_id = :ol_supply_w_id;
		+===============================================*/

		uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
		
		INDEX * stock_index = _wl->i_stock;
		itemid_t * stock_item;
		
		index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
		//<<<<<<<<<<<<<<<<<<<<<<<<<<
		//uint64_t tt_iii = get_sys_clock();
		//INC_INT_STATS(time_phase3_raw, tt_iii - tt_3);
		
		assert(item != NULL);
		row_t * r_stock = ((row_t *)stock_item->location);
		
		uint64_t tt_4 = get_sys_clock();
		INC_INT_STATS(time_phase3, tt_4 - tt_3);
		COMPILER_BARRIER
		char * r_stock_data = NULL;
        rc = get_row(r_stock, WR, r_stock_data); //<<<<<<<<<<<<<<<<
		uint64_t tt_5 = get_sys_clock();
		INC_INT_STATS(time_phase1_1_raw, tt_5 - tt_4);
		if (rc != RCOK)
			return finish(Abort);
		
		// XXX s_dist_xx are not retrieved.
		UInt64 s_quantity;
		int64_t s_remote_cnt;
		s_quantity = *(int64_t *)row_t::get_value(r_stock->get_schema(), S_QUANTITY, r_stock_data);
#if !TPCC_SMALL
		int64_t s_ytd = *(int64_t *)row_t::get_value(r_stock->get_schema(), S_YTD, r_stock_data);
        s_ytd += ol_quantity;
        row_t::set_value(r_stock->get_schema(), S_YTD, r_stock_data, (char *)&s_ytd);

        int64_t s_order_cnt = *(int64_t *)row_t::get_value(r_stock->get_schema(), S_ORDER_CNT, r_stock_data);
        s_order_cnt ++;
        row_t::set_value(r_stock->get_schema(), S_ORDER_CNT, r_stock_data, (char *)&s_order_cnt);
#endif
		uint64_t tt_6 = get_sys_clock();
		INC_INT_STATS(time_phase1_2_raw, tt_6 - tt_5);
		if (remote) {
            s_remote_cnt = *(int64_t*)row_t::get_value(r_stock->get_schema(), S_REMOTE_CNT, r_stock_data);
            s_remote_cnt ++;
            row_t::set_value(r_stock->get_schema(), S_REMOTE_CNT, r_stock_data, (char *)&s_remote_cnt);
		}
		uint64_t quantity;
		if (s_quantity > ol_quantity + 10) {
			quantity = s_quantity - ol_quantity;
		} else {
			quantity = s_quantity - ol_quantity + 91;
		}
		row_t::set_value(r_stock->get_schema(), S_QUANTITY, r_stock_data, (char *)&quantity);
		uint64_t tt_7 = get_sys_clock();
		INC_INT_STATS(time_phase2_raw, tt_7 - tt_6);
		/*====================================================+
		EXEC SQL INSERT
			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
				ol_i_id, ol_supply_w_id,
				ol_quantity, ol_amount, ol_dist_info)
			VALUES(:o_id, :d_id, :w_id, :ol_number,
				:ol_i_id, :ol_supply_w_id,
				:ol_quantity, :ol_amount, :ol_dist_info);
		+====================================================*/
		// XXX district info is not inserted.
#if TPCC_FULL		
		row_t * r_ol;
		uint64_t row_id;
		if(!insert_row(_wl->t_orderline, r_ol, part_id, row_id)) return finish(Abort);
		//_wl->t_orderline->get_new_row(r_ol, 0, row_id);
		r_ol->set_primary_key(orderlineKey(ol_number, o_id, d_id, w_id));
		r_ol->set_value(OL_O_ID, o_id);
		r_ol->set_value(OL_D_ID, d_id);
		r_ol->set_value(OL_W_ID, w_id);
		r_ol->set_value(OL_NUMBER, ol_number);
		r_ol->set_value(OL_I_ID, ol_i_id);
#if !TPCC_SMALL
		int w_tax=1, d_tax=1;
		uint64_t c_discount;
		r_cust->get_value(C_DISCOUNT, c_discount);
		int64_t ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
		const char* ol_dist_info = r_stock->get_value(S_DIST_01 + d_id - 1);
		r_ol->set_value(OL_SUPPLY_W_ID, ol_supply_w_id);
		r_ol->set_value(OL_DELIVERY_D, ol_delivery_d);
		r_ol->set_value(OL_QUANTITY, ol_quantity);
		r_ol->set_value(OL_AMOUNT, ol_amount);
		r_ol->set_value(OL_DIST_INFO, const_cast<char*>(ol_dist_info));
#endif		
#if TPCC_INSERT_INDEX
		{
			auto idx = _wl->i_orderline;
			auto key = orderlineKey(ol_number, o_id, d_id, w_id);
			if (!insert_idx(idx, key, r_ol, part_id, tpcc_wl::IID_ORDERLINE)) {
				// printf("ol_i_id=%d o_id=%d d_id=%d w_id=%d\n", (int)ol_i_id, (int)o_id,
				//        (int)d_id, (int)w_id);
				return finish(Abort);
			}
		}
#endif
		//insert_row(r_ol, _wl->t_orderline);
#endif		
	}
	assert( rc == RCOK );
	if (g_log_recover)
	{
		return RCOK;
	}
	else 
	{
		//assert(insert_cnt != 0);
		return finish(rc);
	}
}

// read-only
RC 
tpcc_txn_man::run_order_status(tpcc_query * query) {
#if TPCC_FULL
	row_t * r_cust __attribute__((unused));
	
	if (query->by_last_name) {
		// EXEC SQL SELECT count(c_id) INTO :namecnt FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
		// EXEC SQL DECLARE c_name CURSOR FOR SELECT c_balance, c_first, c_middle, c_id
		// FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id ORDER BY c_first;
		// EXEC SQL OPEN c_name;
		// if (namecnt%2) namecnt++; / / Locate midpoint customer for (n=0; n<namecnt/ 2; n++)
		// {
		//	   	EXEC SQL FETCH c_name
		//	   	INTO :c_balance, :c_first, :c_middle, :c_id;
		// }
		// EXEC SQL CLOSE c_name;

		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		auto index = _wl->i_customer_last;
		//uint64_t thd_id = get_thd_id();
		/*itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location); */

		row_t* rows[100];
		size_t count = 100;
		auto rc = index_read_multiple(index, key, rows, count, wh_to_part(query->c_w_id));
		if (rc != RCOK) {
			assert(false);
		}
		assert (count != 0);
		assert(count != 100);

		auto mid = rows[count / 2];

		r_cust = mid;
		query->c_id = *(uint64_t *) r_cust->get_value(C_ID); // update C_ID
	} else {
		// EXEC SQL SELECT c_balance, c_first, c_middle, c_last
		// INTO :c_balance, :c_first, :c_middle, :c_last
		// FROM customer
		// WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
		uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		auto index = _wl->i_customer_id;
		//itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		//r_cust = (row_t *) item->location;
		r_cust = search(index, key, wh_to_part(query->c_w_id), RD, true);
	}
#if TPCC_ACCESS_ALL

	row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	r_cust_local->get_value(C_BALANCE, c_balance);
	char * c_first = r_cust_local->get_value(C_FIRST);
	char * c_middle = r_cust_local->get_value(C_MIDDLE);
	char * c_last = r_cust_local->get_value(C_LAST);
#endif
	// Get Last Order

	// EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
	// INTO :o_id, :o_carrier_id, :entdate FROM orders
	// ORDER BY o_id DESC;
	
	//uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
	auto key = orderCustKey(g_max_orderline, query->c_id, query->c_d_id, query->c_w_id);
	auto max_key = orderCustKey(1, query->c_id, query->c_d_id, query->c_w_id);
	auto * index = _wl->i_order_cust;
	
	row_t* rows[16];
  	uint64_t count = 1;
	
	//itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
	auto idx_rc = index_read_range(index, key, max_key, rows, count, wh_to_part(query->c_w_id));
	// printf("order_status_getLastOrder: %" PRIu64 "\n", count);
	if (count == 0) {
		// There must be at least one order per customer.
		printf("order_status_getLastOrder: w_id=%" PRIu64 " d_id=%" PRIu64
			" c_id=%" PRIu64 " count=%" PRIu64 "\n",
			query->c_w_id, query->c_d_id, query->c_id, count);
		assert(false);
	}

	row_t * r_order = rows[0];
	row_t * r_order_local = get_row(r_order, RD);
	if (r_order_local == NULL) {
		//assert(false); //with NO_WAIT, this could happen.
		return finish(Abort);
	}

	uint64_t o_id;
	r_order_local->get_value(O_ID, o_id);
#if TPCC_ACCESS_ALL
	uint64_t o_entry_d, o_carrier_id;
	r_order_local->get_value(O_ENTRY_D, o_entry_d);
	r_order_local->get_value(O_CARRIER_ID, o_carrier_id);
#endif
#if DEBUG_ASSERT
	itemid_t * it = item;
	while (it != NULL && it->next != NULL) {
		uint64_t o_id_1, o_id_2;
		((row_t *)it->location)->get_value(O_ID, o_id_1);
		((row_t *)it->next->location)->get_value(O_ID, o_id_2);
		assert(o_id_1 > o_id_2);
	}
#endif

	// EXEC SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// ol_amount, ol_delivery_d
	// FROM order_line
	// WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// EXEC SQL OPEN c_line;
	// EXEC SQL WHENEVER NOT FOUND CONTINUE;
	// i=0;
	// while (sql_notfound(FALSE)) {
	// 		i++;
	//		EXEC SQL FETCH c_line
	//		INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i], :ol_amount[i], :ol_delivery_d[i];
	// }
	//key = orderlineKey(query->w_id, query->d_id, o_id);
	key = orderlineKey(1, o_id, query->c_d_id, query->w_id);
	max_key = orderlineKey(15, o_id, query->c_d_id, query->w_id);
	index = _wl->i_orderline;

	//row_t* rows[16];
	//uint64_t count = 16;
	count = 16;

	idx_rc = index_read_range(index, key, max_key, rows, count, wh_to_part(query->c_w_id));
	if (idx_rc == Abort) return finish(Abort);
	assert(idx_rc == RCOK);
	assert(count != 16);

	//item = index_read(index, key, wh_to_part(query->w_id));
	//assert(item != NULL);
#if TPCC_ACCESS_ALL
	// TODO the rows are simply read without any locking mechanism
	/*while (item != NULL) {
		row_t * r_orderline = (row_t *) item->location;
		int64_t ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d;
		r_orderline->get_value(OL_I_ID, ol_i_id);
		r_orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
		r_orderline->get_value(OL_QUANTITY, ol_quantity);
		r_orderline->get_value(OL_AMOUNT, ol_amount);
		r_orderline->get_value(OL_DELIVERY_D, ol_delivery_d);
		item = item->next;
	}*/
	for (uint64_t i = 0; i < count; i++) {
		auto shared = rows[i];
		auto local = get_row(index, shared, part_id, RD);
		if (local == NULL) return finish(Abort);
		(void)local;
	}
#endif

//final:
//	assert( rc == RCOK );
//	return finish(rc);
#endif
	//assert( rc == RCOK );
	assert(wr_cnt ==0); // read-only
	if (g_log_recover)
		return RCOK;
	else 
		return finish(RCOK);
	//return RCOK;
}


//////////////////////////////////////////////////////
// Delivery
//////////////////////////////////////////////////////

bool tpcc_txn_man::delivery_getNewOrder_deleteNewOrder(uint64_t d_id,
                                                       uint64_t w_id,
                                                       int64_t* out_o_id) {
  // SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1
  // DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?

  auto index = _wl->i_neworder;
  // TODO: This may cause a match with other district with a negative order ID.  It is safe for now because the lowest order ID is 1, but we should give more gap (or use tuple keys) to avoid accidental matches.
  auto key = neworderKey(g_max_orderline, d_id, w_id);
#ifndef TPCC_SILO_REF_LAST_NO_O_IDS
  auto max_key = neworderKey(0, d_id, w_id);  // Use key ">= 0" for "> -1"
#else
  // Use reference Silo's last seen o_id history.
  auto max_key = neworderKey(
      last_no_o_ids[(w_id - 1) * DIST_PER_WARE + d_id - 1].o_id, d_id, w_id);
#endif
  auto part_id = wh_to_part(w_id);

  row_t* rows[1];
  uint64_t count = 1;

  auto idx_rc = index_read_range_rev(index, key, max_key, rows, count, part_id);
  if (idx_rc == Abort) return false;
  assert(idx_rc == RCOK);

  // printf("delivery_getNewOrder_deleteNewOrder: %" PRIu64 "\n", count);
  if (count == 0) {
    // No new order; this is acceptable and we do not need to abort TX.
    *out_o_id = -1;
    return true;
  }

  auto shared = rows[0];
  char * data;
  auto local = get_row(shared, WR, data);
  if (local != RCOK) return false;

  int64_t o_id;
  shared->get_value(NO_O_ID, o_id);
  *out_o_id = o_id;

#ifdef TPCC_SILO_REF_LAST_NO_O_IDS
  last_no_o_ids[(w_id - 1) * DIST_PER_WARE + d_id - 1].o_id = o_id + 1;
#endif

#if TPCC_DELETE_INDEX
  {
    auto idx = _wl->i_neworder;
    auto key = neworderKey(o_id, d_id, w_id);
// printf("o_id=%" PRIi64 " d_id=%" PRIu64 " w_id=%" PRIu64 "\n", o_id, d_id,
//        w_id);
// printf("requesting remove_idx idx=%p key=%" PRIu64 " row_id=%" PRIu64 " part_id=%" PRIu64 " \n",
//        idx, key, (uint64_t)rows[0], part_id);
    if (!remove_idx(idx, key, rows[0], part_id, tpcc_wl::IID_NEWORDER)) return false;

  }
#endif

#if TPCC_DELETE_ROWS
  if (!remove_row(shared)) return false;

#endif

  return true;
}

row_t* tpcc_txn_man::delivery_getCId(int64_t no_o_id, uint64_t d_id,
                                     uint64_t w_id) {
  // SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
  auto index = _wl->i_order;
  auto key = orderKey(no_o_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);

  row_t* row;
  auto ret = index_read(index, key, &row, part_id);
  if(ret!=RCOK)
  {
	  INC_INT_STATS(int_search_abort_notfound, 1);
	  return NULL;
  }
  char* data;
  auto rc = get_row(row, WR, data); // trace the read/write
  if(rc!=RCOK)
  {
	  INC_INT_STATS(int_search_abort_lock, 1);
	  return NULL;
  }
  return row;
  //return search(index, key, part_id, WR);

}

void tpcc_txn_man::delivery_updateOrders(row_t* row, uint64_t o_carrier_id) {
  // UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?
  row->set_value(O_CARRIER_ID, o_carrier_id);
}

bool tpcc_txn_man::delivery_updateOrderLine_sumOLAmount(uint64_t o_entry_d,
                                                        int64_t no_o_id,
                                                        uint64_t d_id,
                                                        uint64_t w_id,
                                                        double* out_ol_total) {
  // UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?
  // SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
  double ol_total = 0.0;

  auto index = _wl->i_orderline;
  auto key = orderlineKey(1, no_o_id, d_id, w_id);
  auto max_key = orderlineKey(15, no_o_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);

  row_t* rows[16];
  uint64_t count = 16;

  auto idx_rc = index_read_range(index, key, max_key, rows, count, part_id);
  if (idx_rc != RCOK) return false;
  assert(count != 16);

  for (uint64_t i = 0; i < count; i++) {
    auto shared = rows[i];
	char * data;
    auto local = get_row(shared, WR, data); // TODO: from get_row with index
    if (local != RCOK) return false;
    double ol_amount;
    rows[i]->get_value(OL_AMOUNT, ol_amount);
    rows[i]->set_value(OL_DELIVERY_D, o_entry_d);
    ol_total += ol_amount;
  }

  // printf("delivery_updateOrderLine_sumOLAmount: w_id=%" PRIu64 " d_id=%" PRIu64
  //        " o_id=%" PRIu64 " cnt=%" PRIu64 "\n",
  //        w_id, d_id, no_o_id, cnt);
  *out_ol_total = ol_total;
  return true;
}

bool tpcc_txn_man::delivery_updateCustomer(double ol_total, uint64_t c_id,
                                           uint64_t d_id, uint64_t w_id) {
  // UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?
  auto index = _wl->i_customer_id;
  auto key = custKey(c_id, d_id, w_id);
  auto part_id = wh_to_part(w_id);
#if !TPCC_CF
  auto row = search(index, key, part_id, WR); // search function combines index read and get_row
#else
  const access_t cf_access_type[] = {SKIP, WR, SKIP};
  auto row = search(index, key, part_id, SKIP, cf_access_type);
#endif
  if (row == NULL) return false;

  double c_balance;
  uint64_t c_delivery_cnt;
  row->get_value(C_BALANCE, c_balance);
  row->set_value(C_BALANCE, c_balance + ol_total);
  row->get_value(C_DELIVERY_CNT, c_delivery_cnt);
  row->set_value(C_DELIVERY_CNT, c_delivery_cnt + 1);
  return true;
}

RC tpcc_txn_man::run_delivery(tpcc_query* query) {
#if TPCC_FULL

// DBx1000's active delivery transaction checking.
#if TPCC_DBX1000_SERIAL_DELIVERY
  if (__sync_lock_test_and_set(&active_delivery[query->w_id - 1].lock, 1) == 1)
    return finish(RCOK);
#endif

#if !TPCC_SPLIT_DELIVERY
  for (uint64_t d_id = 1; d_id <= DIST_PER_WARE; d_id++)
#else
  for (uint64_t d_id = query->sub_query_id + 1; d_id == query->sub_query_id + 1;
       d_id++)
#endif
  {
	uint64_t t_start = get_sys_clock();
    int64_t o_id;
    if (!delivery_getNewOrder_deleteNewOrder(d_id, query->w_id, &o_id)) {
      // printf("oops0\n");
#if TPCC_DBX1000_SERIAL_DELIVERY
      __sync_lock_release(&active_delivery[query->w_id - 1].lock);
#endif
      // INC_STATS_ALWAYS(get_thd_id(), debug1, 1);
	  INC_INT_STATS(int_delivery_abort_deleteNO, 1);
      return finish(Abort);
    }
    // No new order for this district.
    if (o_id == -1) {
      // printf("skipping w_id=%" PRIu64 " d_id=%" PRIu64 " for no new order\n",
      //        arg.w_id, d_id);
      continue;
    }
	uint64_t t_after_deleteNew_Order = get_sys_clock();
	INC_INT_STATS(time_delivery_deleteOrder, t_after_deleteNew_Order - t_start);

    auto order = delivery_getCId(o_id, d_id, query->w_id);
    if (order == NULL) {
      // There is no guarantee that we will see a order row even after seeing a related new_order row in this read-write transaction.
      // printf("oops1\n");
	  INC_INT_STATS(int_delivery_abort_getCID, 1);
	  if(!g_log_recover)
      	return finish(Abort);
	  return RCOK;
    }
    uint64_t c_id;
    order->get_value(O_C_ID, c_id);

	uint64_t t_after_getCID = get_sys_clock();
	INC_INT_STATS(time_delivery_getCID, t_after_getCID - t_after_deleteNew_Order);

    delivery_updateOrders(order, query->o_carrier_id);

	uint64_t t_after_updateOrders = get_sys_clock();
	INC_INT_STATS(time_delivery_UpdateOrder, t_after_updateOrders - t_after_getCID);

    double ol_total;
#ifndef TPCC_CAVALIA_NO_OL_UPDATE
    if (!delivery_updateOrderLine_sumOLAmount(query->ol_delivery_d, o_id, d_id,
                                              query->w_id, &ol_total)) {
      // printf("oops2\n");
#if TPCC_DBX1000_SERIAL_DELIVERY
      __sync_lock_release(&active_delivery[query->w_id - 1].lock);
#endif
	  INC_INT_STATS(int_delivery_abort_updateOL, 1);
      // INC_STATS_ALWAYS(get_thd_id(), debug2, 1);
      return finish(Abort);
    }
#else
    ol_total = 1.;
#endif

	uint64_t t_after_updateOrderline = get_sys_clock();
	INC_INT_STATS(time_delivery_updateOrderLine, t_after_updateOrderline - t_after_updateOrders);

    if (!delivery_updateCustomer(ol_total, c_id, d_id, query->w_id)) {
      // printf("oops3\n");
#if TPCC_DBX1000_SERIAL_DELIVERY
      __sync_lock_release(&active_delivery[arg.w_id - 1].lock);
#endif
	  INC_INT_STATS(int_delivery_abort_updateCustomer, 1);
      // INC_STATS_ALWAYS(get_thd_id(), debug3, 1);
      return finish(Abort);
    }
	uint64_t t_after_updateCustomer = get_sys_clock();
  	INC_INT_STATS(time_delivery_updateCustomer, t_after_updateCustomer - t_after_updateOrderline);
  }
  
  //auto rc = finish(RCOK);
// if (rc != RCOK) INC_STATS_ALWAYS(get_thd_id(), debug4, 1);
#if TPCC_DBX1000_SERIAL_DELIVERY
  __sync_lock_release(&active_delivery[arg.w_id - 1].lock);
#endif
    if (g_log_recover)
		return RCOK;
    else 
		return finish(RCOK);

#else  // !TPCC_FULL
  assert(false); // IF NOT TPCC_FULL, THIS SHOULD NOT BE RUNNING.
  return finish(RCOK);
#endif
}


//////////////////////////////////////////////////////
// Stock Level
//////////////////////////////////////////////////////

row_t* tpcc_txn_man::stock_level_getOId(uint64_t d_w_id, uint64_t d_id) {
  // SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?
  auto index = _wl->i_district;
  auto key = distKey(d_id, d_w_id);
  auto part_id = wh_to_part(d_w_id);

  return search(index, key, part_id, RD);

}

bool tpcc_txn_man::stock_level_getStockCount(uint64_t ol_w_id, uint64_t ol_d_id,
                                             int64_t ol_o_id, uint64_t s_w_id,
                                             uint64_t threshold,
                                             uint64_t* out_distinct_count) {
  // SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
  // WHERE OL_W_ID = ?
  //   AND OL_D_ID = ?
  //   AND OL_O_ID < ?
  //   AND OL_O_ID >= ?
  //   AND S_W_ID = ?
  //   AND S_I_ID = OL_I_ID
  //   AND S_QUANTITY < ?

  // 20 orders * 15 items = 300; use 301 to check any errors.
  uint64_t ol_i_id_list[301];
  size_t list_size = 0;

  auto index = _wl->i_orderline;
  auto key = orderlineKey(1, ol_o_id - 1, ol_d_id, ol_w_id);
  auto max_key = orderlineKey(15, ol_o_id - 20, ol_d_id, ol_w_id);
  auto part_id = wh_to_part(ol_w_id);

  row_t* rows[301];
  uint64_t count = 301;

  auto idx_rc = index_read_range(index, key, max_key, rows, count, part_id);

  if (idx_rc == Abort) return false;

  assert(idx_rc == RCOK);

  for (uint64_t i = 0; i < count; i++) {
    auto orderline_shared = rows[i];
	char *data;
    auto orderline = get_row(orderline_shared, RD, data); // TODO, previously get_row with index

    if (orderline != RCOK) return false;

    uint64_t ol_i_id, ol_supply_w_id;
    orderline_shared->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
    if (ol_supply_w_id != s_w_id) continue;

    orderline_shared->get_value(OL_I_ID, ol_i_id);

    assert(list_size < sizeof(ol_i_id_list) / sizeof(ol_i_id_list[0]));
    ol_i_id_list[list_size] = ol_i_id;
    list_size++;
  }
  assert(list_size <= 300);

#ifndef TPCC_FOEDUS_DUPLICATE_ITEM
  uint64_t distinct_ol_i_id_list[300];
  uint64_t distinct_ol_i_id_count = 0;
#endif
  uint64_t result = 0;
  // std::unordered_set<uint64_t> ol_i_id_set(300 * 2);

  for (uint64_t i = 0; i < list_size; i++) {
    uint64_t ol_i_id = ol_i_id_list[i];

#ifndef TPCC_FOEDUS_DUPLICATE_ITEM
    bool duplicate = false;
    for (uint64_t j = 0; j < distinct_ol_i_id_count; j++)
      if (distinct_ol_i_id_list[j] == ol_i_id) {
        duplicate = true;
        break;
      }
    if (duplicate) continue;

    distinct_ol_i_id_list[distinct_ol_i_id_count++] = ol_i_id;
#endif

    // auto it = ol_i_id_set.find(ol_i_id);
    // if (it != ol_i_id_set.end()) continue;
    // ol_i_id_set.emplace_hint(it, ol_i_id);

    auto key = stockKey(ol_i_id, s_w_id);
    auto index = _wl->i_stock;
    auto part_id = wh_to_part(s_w_id);

    auto row = search(index, key, part_id, RD);

    if (row == NULL) return false;

    uint64_t s_quantity;
    row->get_value(S_QUANTITY, s_quantity);
    if (s_quantity < threshold) result++;
  }

  // printf("stock_level_getStockCount: w_id=%" PRIu64 " d_id=%" PRIu64
  //        " o_id=%" PRIu64 " s_w_id=%" PRIu64 " list_size=%" PRIu64
  //        " distinct_cnt=%" PRIu64 " result=%" PRIu64 "\n",
  //        ol_w_id, ol_d_id, ol_o_id, s_w_id, list_size, distinct_ol_i_id_count,
  //        result);
  *out_distinct_count = result;
  return true;
}

RC tpcc_txn_man::run_stock_level(tpcc_query* query) {
	
#if TPCC_FULL

	auto district = stock_level_getOId(query->w_id, query->d_id);
	if (district == NULL) {
		return finish(Abort);
	}
	int64_t o_id;
	district->get_value(D_NEXT_O_ID, o_id);

	uint64_t distinct_count;
	if (!stock_level_getStockCount(query->w_id, query->d_id, o_id, query->w_id,
									query->threshold, &distinct_count)) {
		return finish(Abort);
	}
	(void)distinct_count;
	assert(wr_cnt ==0); // read-only
    if (g_log_recover)
		return RCOK;
    else 
		return finish(RCOK);
#else
	assert(false);
	return finish(RCOK);
#endif

  
}




//////////////////////////////////////////////////////
void 
tpcc_txn_man::get_cmd_log_entry()
{
	// Format
	//  | stored_procedure_id | input_params
	PACK(_log_entry, _query->type, _log_entry_size);
	if (_query->type == TPCC_PAYMENT) {
		// format
        //  w_id | d_id | c_id |
        //  d_w_id | c_w_id | c_d_id |
        //  h_amount | by_last_name | c_last[LASTNAME_LEN] 
		PACK(_log_entry, _query->w_id, _log_entry_size);
		PACK(_log_entry, _query->d_id, _log_entry_size);
		PACK(_log_entry, _query->c_id, _log_entry_size);
		
		PACK(_log_entry, _query->d_w_id, _log_entry_size);
		PACK(_log_entry, _query->c_w_id, _log_entry_size);
		PACK(_log_entry, _query->c_d_id, _log_entry_size);
		
		PACK(_log_entry, _query->h_amount, _log_entry_size);
		PACK(_log_entry, _query->by_last_name, _log_entry_size);
		PACK_SIZE(_log_entry, _query->c_last, LASTNAME_LEN, _log_entry_size);
	} else if (_query->type == TPCC_NEW_ORDER) {
        // format
        //  uint64_t w_id | uint64_t d_id | uint64_t c_id |
        //  bool remote | uint64_t ol_cnt | uint64_t o_entry_d |
        //  Item_no * ol_cnt
		PACK(_log_entry, _query->w_id, _log_entry_size);
		PACK(_log_entry, _query->d_id, _log_entry_size);
		PACK(_log_entry, _query->c_id, _log_entry_size);

		PACK(_log_entry, _query->remote, _log_entry_size);
		PACK(_log_entry, _query->ol_cnt, _log_entry_size);
		PACK(_log_entry, _query->o_entry_d, _log_entry_size);
		
		PACK_SIZE(_log_entry, _query->items, sizeof(Item_no) * _query->ol_cnt, _log_entry_size);
	} else if (_query->type == TPCC_DELIVERY) {
		PACK(_log_entry, _query->w_id, _log_entry_size);
		PACK(_log_entry, _query->o_carrier_id, _log_entry_size);
	} else {
		assert(false); // the rest two transactions are read-only
	}
}

uint32_t 
tpcc_txn_man::get_cmd_log_entry_length()
{
	// Format
	//  | stored_procedure_id | input_params
	uint32_t ret;
	ret = sizeof(TPCCTxnType);

	if (_query->type == TPCC_PAYMENT) {
		// format
        //  w_id | d_id | c_id |
        //  d_w_id | c_w_id | c_d_id |
        //  h_amount | by_last_name | c_last[LASTNAME_LEN] 
		ret += sizeof(uint64_t) * 6 + sizeof(double) + sizeof(bool) + LASTNAME_LEN;
		
	} else if (_query->type == TPCC_NEW_ORDER) {
        // format
        //  uint64_t w_id | uint64_t d_id | uint64_t c_id |
        //  bool remote | uint64_t ol_cnt | uint64_t o_entry_d |
        //  Item_no * ol_cnt

		ret += sizeof(uint64_t) * 3 + sizeof(bool) + sizeof(uint64_t) * 2 + sizeof(Item_no) * _query->ol_cnt;
	}
	return ret;
}

void
tpcc_txn_man::recover_txn(char * log_entry, uint64_t tid)
{
	uint64_t tt = get_sys_clock();
#if LOG_TYPE == LOG_DATA
	// Format 
	// 	| N | (table_id | primary_key | data_length | data) * N
	uint32_t offset = 0;


#if TPCC_FULL
	#if TPCC_INSERT_ROWS
	uint64_t insert_num;
	UNPACK(log_entry, insert_num, offset);
	row_t *inserted_rows[insert_num];
	//cout << "insertion not implemented. " << endl;
	for (uint32_t i=0; i < insert_num; i++)
	{
		uint32_t table_id;
		uint64_t key;
		uint64_t part_id;
		
		char * data;
		UNPACK(log_entry, table_id, offset);
		UNPACK(log_entry, key, offset);
		UNPACK(log_entry, part_id, offset);
		
		assert(table_id < NUM_TABLES);
		uint64_t row_id;
		auto ret = _wl->tpcc_tables[(TableName)table_id]->
			get_new_row(inserted_rows[i], part_id, row_id);
		assert(ret==RCOK); // if the logging algorithm is corret, this must succeed.
		inserted_rows[i]->set_primary_key(key);
		//printf("[%lu] recover insert table %u, row_key %lu\n", get_thd_id(), table_id, key);
	}
	#endif
	#if TPCC_INSERT_INDEX
	uint64_t insert_idx_num;
	UNPACK(log_entry, insert_idx_num, offset);
	for(uint32_t i=0; i< insert_idx_num; i++)
	{
		int idx_ind;
		idx_key_t idx_key;
		uint32_t idx_row_id;
		int part_id;
		UNPACK(log_entry, idx_ind, offset);
		UNPACK(log_entry, idx_key, offset);
		UNPACK(log_entry, idx_row_id, offset);
		UNPACK(log_entry, part_id, offset);
		ORDERED_INDEX* oid;
		switch(idx_ind)
		{
			case tpcc_wl::IID_ORDER:
				oid = _wl->i_order;
				break;
			case tpcc_wl::IID_ORDER_CUST:
				oid = _wl->i_order_cust;
				break;
			case tpcc_wl::IID_NEWORDER:
				oid = _wl->i_neworder;
				break;
			case tpcc_wl::IID_ORDERLINE:
				oid = _wl->i_orderline;
				break;
			default:
				assert(false);
		}
		//printf("[%lu] recover insert id %u, row_id %u, row_key %lu\n", get_thd_id(), idx_ind, idx_row_id, idx_key);
		auto rc_insert = oid->index_insert(this, idx_key, inserted_rows[idx_row_id], part_id);
		assert(rc_insert == RCOK);
	}
	#endif
#endif

	uint32_t num_keys; 
	UNPACK(log_entry, num_keys, offset);
	for (uint32_t i = 0; i < num_keys; i ++) {
		uint64_t t2 = get_sys_clock();
		uint32_t table_id;
		uint64_t key;
		uint32_t data_length;
		char * data;

		UNPACK(log_entry, table_id, offset);
		UNPACK(log_entry, key, offset);
		UNPACK(log_entry, data_length, offset);
		assert(data_length!=0);
		data = log_entry + offset;
		offset += data_length;
		assert(table_id < NUM_TABLES);
		row_t * row;
		if(table_id <= SPLIT_ORDERED_PRIMARY_INDEX)
		{
			// a hash index
			itemid_t * m_item = index_read(
			_wl->tpcc_tables[(TableName)table_id]->get_primary_index(),
			key, 0);
			row = ((row_t *)m_item->location);
		}
		else
		{
			// an ordered index
			ORDERED_INDEX* oi = _wl->tpcc_tables[(TableName)table_id]->get_primary_ordered_index();
			assert(g_part_cnt==1);
			auto rc = oi->index_read(this, key, &row, 0);
			if(rc!=RCOK)
			{
				//printf("[%lu] index not found, table %u, key %lu\n", get_thd_id(), table_id, key);
				assert(false);
			}
			
		}
		
		uint64_t t3 = get_sys_clock();
		INC_INT_STATS(time_debug5, t3 - t2);
		
	#if LOG_ALGORITHM == LOG_BATCH
		row->manager->lock();
		uint64_t cur_tid = row->manager->get_tid();
		if (tid > cur_tid) { 
			row->set_data(data, data_length);
			row->manager->set_tid(tid);
		}
		row->manager->release();
	#else
		row->set_data(data, data_length);
	#endif
		INC_INT_STATS(time_debug6, get_sys_clock() - t3);
	}

#if TPCC_FULL
	#if TPCC_DELETE_ROWS
	/*
	uint64_t remove_num;
	UNPACK(log_entry, remove_num, offset);
	for(uint32_t i=0; i< remove_num; i++)
	{
		uint32_t table_id;
		uint64_t key;
		UNPACK(log_entry, table_id, offset);
		UNPACK(log_entry, key, offset);
		// DBx1000 assumes lazy deletion. 
		// We just remove the index so later query will not see the row.
	}
	*/
	//cout << "deletion not implemented. " << endl;
	#endif
	#if TPCC_DELETE_INDEX
	uint64_t remove_idx_num;
	UNPACK(log_entry, remove_idx_num, offset);
	for(uint32_t i=0; i < remove_idx_num; i++)
	{
		int idx_ind;
		idx_key_t idx_key;
		int part_id;
		UNPACK(log_entry, idx_ind, offset);
		UNPACK(log_entry, idx_key, offset);
		UNPACK(log_entry, part_id, offset);
		ORDERED_INDEX* oid;
		switch(idx_ind)
		{
			case tpcc_wl::IID_ORDER:
				oid = _wl->i_order;
				break;
			case tpcc_wl::IID_ORDER_CUST:
				oid = _wl->i_order_cust;
				break;
			case tpcc_wl::IID_NEWORDER:
				oid = _wl->i_neworder;
				break;
			case tpcc_wl::IID_ORDERLINE:
				oid = _wl->i_orderline;
				break;
			default:
				assert(false);
		}
		auto rc_remove = oid->index_remove(this, idx_key, NULL, part_id);
		assert(rc_remove == RCOK);
	}
	#endif


#endif


#elif LOG_TYPE == LOG_COMMAND
	if (!_query) {
		_query = new tpcc_query;
		_query->items = new Item_no [15];
	}
	// format
	//  | stored_procedure_id | w_id | d_id | c_id | txn_specific_info
	// txn_specific_info includes
	// For Payment
    //  uint64_t d_w_id | uint64_t c_w_id | uint64_t c_d_id |
    //  double h_amount | bool by_last_name | char c_last[LASTNAME_LEN] 
	// For New-Order
    //  bool remote | uint64_t ol_cnt | uint64_t o_entry_d |
    //  Item_no * ol_cnt
	uint64_t offset = 0;
	UNPACK(log_entry, _query->type, offset);
	
	if (_query->type == TPCC_PAYMENT) {
		UNPACK(log_entry, _query->w_id, offset);
		UNPACK(log_entry, _query->d_id, offset);
		UNPACK(log_entry, _query->c_id, offset);
		UNPACK(log_entry, _query->d_w_id, offset);
		UNPACK(log_entry, _query->c_w_id, offset);
		UNPACK(log_entry, _query->c_d_id, offset);
		UNPACK(log_entry, _query->h_amount, offset);
		UNPACK(log_entry, _query->by_last_name, offset);
		UNPACK_SIZE(log_entry, _query->c_last, LASTNAME_LEN, offset);
		//assert(_query->c_id <= g_cust_per_dist);

	} else if (_query->type == TPCC_NEW_ORDER) {
		UNPACK(log_entry, _query->w_id, offset);
		UNPACK(log_entry, _query->d_id, offset);
		UNPACK(log_entry, _query->c_id, offset);
		UNPACK(log_entry, _query->remote, offset);
		UNPACK(log_entry, _query->ol_cnt, offset);
		UNPACK(log_entry, _query->o_entry_d, offset);
		UNPACK_SIZE(log_entry, _query->items, sizeof(Item_no) * _query->ol_cnt, offset);
		
		//assert(_query->c_id <= g_cust_per_dist);
	} else if (_query->type == TPCC_DELIVERY){
		UNPACK(log_entry, _query->w_id, offset);
		UNPACK(log_entry, _query->o_carrier_id, offset);
		_query->ol_delivery_d = 2013;
	} else {
		assert(false);
	}
	
	run_txn(_query);
#endif
	INC_INT_STATS(time_recover_txn, get_sys_clock() - tt);
}

void 
tpcc_query::print()
{
	printf("Type=%d, w_id=%" PRIu64 ", d_id=%" PRIu64 ", c_id=%" PRIu64 ", by_last_name=%d\n",
		type, w_id, d_id, c_id, by_last_name);	
}
