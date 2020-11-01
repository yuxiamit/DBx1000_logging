#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "index_array.h"
#include "index_mbtree.h"
#include "index_btree.h"
#include "catalog.h"
#include "mem_alloc.h"

RC workload::init() {
	sim_done = 0;
	return RCOK;
}

RC workload::init_schema(string schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);	
	  string line;
	  ifstream fin(schema_file.c_str());
    Catalog * schema;
	//uint32_t table_id = 0;
    while (getline(fin, line)) {
    if (line.compare(0, 6, "TABLE=") == 0) {
      string tname;
      tname = &line[6];
      schema = (Catalog*)mem_allocator.alloc(sizeof(Catalog), -1);
      getline(fin, line);
      int col_count = 0;
      // Read all fields for this table.
      vector<string> lines;
      while (line.length() > 1) {
        lines.push_back(line);
        getline(fin, line);
      }
      schema->init(tname.c_str(), lines.size());
      for (UInt32 i = 0; i < lines.size(); i++) {
        string line = lines[i];
        size_t pos = 0;
        string token;
        int elem_num = 0;
        int size = 0;
        string type;
        string name;
        //int cf = 0;
        while (line.length() != 0) {
          pos = line.find(",");
          if (pos == string::npos) pos = line.length();
          token = line.substr(0, pos);
          line.erase(0, pos + 1);
          switch (elem_num) {
            case 0:
              size = atoi(token.c_str());
              break;
            case 1:
              type = token;
              break;
            case 2:
              name = token;
              break;
            case 3:
              // ignore the cf option
              break;
            default:
              assert(false);
          }
          elem_num++;
        }

        assert(elem_num == 3 || elem_num == 4);
        schema->add_col((char*)name.c_str(), size, (char*)type.c_str());
        col_count++;
      }

      int part_cnt = (CENTRAL_INDEX) ? 1 : g_part_cnt;
#if WORKLOAD == TPCC
      if (tname == "ITEM") part_cnt = 1;
#endif

//#if WORKLOAD == YCSB
//     assert(schema->get_tuple_size() == MAX_TUPLE_SIZE);
//#endif

      table_t* cur_tab = (table_t*)mem_allocator.alloc(sizeof(table_t), -1);
      new (cur_tab) table_t;
      cur_tab->init(schema, part_cnt);
      //printf("schema tuple size %u\n", schema->get_tuple_size());
      assert(schema->get_tuple_size() <= MAX_TUPLE_SIZE);
      tables[tname] = cur_tab;
    } else if (!line.compare(0, 6, "INDEX=")) {
      string iname;
      iname = &line[6];
      getline(fin, line);

      vector<string> items;
      string token;
      size_t pos;
      while (line.length() != 0) {
        pos = line.find(",");
        if (pos == string::npos) pos = line.length();
        token = line.substr(0, pos);
        items.push_back(token);
        line.erase(0, pos + 1);
      }

      string tname(items[0]);

      int part_cnt = (CENTRAL_INDEX) ? 1 : g_part_cnt;
#if WORKLOAD == TPCC
      if (tname == "ITEM") part_cnt = 1;
#endif

      uint64_t table_size;
#if WORKLOAD == YCSB
      table_size = g_synth_table_size;
#elif WORKLOAD == TPCC
      if (tname == "ITEM")
        table_size = stoi(items[1]);
      else
        table_size = stoi(items[1]) * g_num_wh;
#elif WORKLOAD == TATP
      table_size = stoi(items[1]) * TATP_SCALE_FACTOR;
#endif

      if (strncmp(iname.c_str(), "ORDERED_", 8) == 0) {
        ORDERED_INDEX* index =
            (ORDERED_INDEX*)mem_allocator.alloc(sizeof(ORDERED_INDEX), -1);
        new (index) ORDERED_INDEX();

        index->init(part_cnt, tables[tname]);
        ordered_indexes[iname] = index;
      } else if (strncmp(iname.c_str(), "ARRAY_", 6) == 0) {
        ARRAY_INDEX* index = (ARRAY_INDEX*)mem_allocator.alloc(sizeof(ARRAY_INDEX), -1);
        new (index) ARRAY_INDEX();

        index->init(part_cnt, tables[tname], table_size * 2);
        array_indexes[iname] = index;
      } else if (strncmp(iname.c_str(), "HASH_", 5) == 0) {
        HASH_INDEX* index = (HASH_INDEX*)mem_allocator.alloc(sizeof(HASH_INDEX), -1);
        new (index) HASH_INDEX();

#if INDEX_STRUCT == IDX_HASH
        index->init(part_cnt, tables[tname], table_size * 2);
#else
        index->init(part_cnt, tables[tname]);
#endif
        hash_indexes[iname] = index;
      }
      else {
        printf("unrecognized index type for %s\n", iname.c_str());
        assert(false);
      }
    }
  }
  fin.close();
	return RCOK;
}


