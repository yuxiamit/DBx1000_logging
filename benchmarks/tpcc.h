#ifndef _TPCC_H_
#define _TPCC_H_

#include "wl.h"
#include "txn.h"
#include "tpcc_const.h"

class table_t;
class INDEX;
class tpcc_query;

class tpcc_wl : public workload {
public:
	RC init();
	RC init_table();
	RC init_schema(string schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	table_t * 		t_warehouse;
	table_t * 		t_district;
	table_t * 		t_customer;
	table_t *		t_history;
	table_t *		t_neworder;
	table_t *		t_order;
	table_t *		t_orderline;
	table_t *		t_item;
	table_t *		t_stock;
/*
	INDEX * 	i_item;
	INDEX * 	i_warehouse;
	INDEX * 	i_district;
	INDEX * 	i_customer_id;
	INDEX * 	i_customer_last;
	INDEX * 	i_stock;
	INDEX * 	i_order; // key = (w_id, d_id, o_id)
	INDEX * 	i_orderline; // key = (w_id, d_id, o_id)
	INDEX * 	i_orderline_wd; // key = (w_id, d_id). 
	*/
	HASH_INDEX* i_item;
	HASH_INDEX* i_warehouse;
	HASH_INDEX* i_district;
	HASH_INDEX* i_customer_id;
	HASH_INDEX* i_customer_last;
	HASH_INDEX* i_stock;
	ORDERED_INDEX* i_order;
	ORDERED_INDEX* i_order_cust;
	ORDERED_INDEX* i_neworder;
	ORDERED_INDEX* i_orderline;

	enum I_INDEX_ID {
		IID_ITEM,
		IID_WAREHOUSE,
		IID_DISTRICT,
		IID_CUSTOMER_ID,
		IID_CUSTOMER_LAST,
		IID_STOCK,
		IID_ORDER,
		IID_ORDER_CUST,
		IID_NEWORDER,
		IID_ORDERLINE,
	};
	
	bool ** delivering;
	uint32_t next_tid;
	
	map<TableName, table_t *> tpcc_tables;
private:
	uint64_t num_wh;
	void init_tab_item();
	void init_tab_wh(uint32_t wid);
	void init_tab_dist(uint64_t w_id);
	void init_tab_stock(uint64_t w_id);
	void init_tab_cust(uint64_t d_id, uint64_t w_id);
	void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
	void init_tab_order(uint64_t d_id, uint64_t w_id);
	
	void init_permutation(uint64_t * perm_c_id, uint64_t wid);

	static void * threadInitItem(void * This);
	static void * threadInitWh(void * This);
	static void * threadInitDist(void * This);
	static void * threadInitStock(void * This);
	static void * threadInitCust(void * This);
	static void * threadInitHist(void * This);
	static void * threadInitOrder(void * This);

	static void * threadInitWarehouse(void * This);
};

class tpcc_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(base_query * query, bool rec=false);
	
	void get_cmd_log_entry();
	uint32_t get_cmd_log_entry_length();
private:
	tpcc_wl * _wl;
	RC run_payment(tpcc_query * m_query);
	RC run_new_order(tpcc_query * m_query);
	RC run_order_status(tpcc_query * query);
	RC run_delivery(tpcc_query * query);
	RC run_stock_level(tpcc_query * query);

#if TPCC_DBX1000_SERIAL_DELIVERY
  struct ActiveDelivery {
    uint32_t lock;
  } __attribute__((aligned(CL_SIZE)));

  ActiveDelivery active_delivery[NUM_WH];
#endif

	tpcc_query * _query; 	
	//TPCCTxnType	_txn_type;
	bool delivery_getNewOrder_deleteNewOrder(uint64_t d_id, uint64_t w_id,
                                           int64_t* out_o_id);
	row_t* delivery_getCId(int64_t no_o_id, uint64_t d_id, uint64_t w_id);
	void delivery_updateOrders(row_t* row, uint64_t o_carrier_id);
	bool delivery_updateOrderLine_sumOLAmount(uint64_t o_entry_d, int64_t no_o_id,
												uint64_t d_id, uint64_t w_id,
												double* out_ol_total);
	bool delivery_updateCustomer(double ol_total, uint64_t c_id, uint64_t d_id,
								uint64_t w_id);

	row_t* stock_level_getOId(uint64_t d_w_id, uint64_t d_id);
	bool stock_level_getStockCount(uint64_t ol_w_id, uint64_t ol_d_id,
									int64_t ol_o_id, uint64_t s_w_id,
									uint64_t threshold,
									uint64_t* out_distinct_count);

	void recover_txn(char * log_entry, uint64_t tid = (uint64_t)-1);
};

#endif
