// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <iostream>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/table.h"

#include "sys/types.h"
#include "sys/sysinfo.h"

using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "/tmp/rocksdb_transaction_example";

void createColumnFamilies(const int columnFamilyCount, const ColumnFamilyOptions &columnFamilyOptions,
                          std::vector<ColumnFamilyDescriptor> &column_families);

int main() {

    DBOptions dbOptions;
    dbOptions.create_if_missing = true;
    dbOptions.create_missing_column_families = true;

    dbOptions.statistics = CreateDBStatistics();
    dbOptions.dump_malloc_stats = true;

    // index and bloom filter
    // table option
    BlockBasedTableOptions basedTableOptions = BlockBasedTableOptions();
    basedTableOptions.block_size = 32 * 1024 * 1024;
    auto cache = NewLRUCache(64 * 1024 * 1024);
    basedTableOptions.block_cache = cache;
    basedTableOptions.cache_index_and_filter_blocks = true;
    basedTableOptions.pin_l0_filter_and_index_blocks_in_cache = true;

    // mem table
//    WriteBufferManager* manager = new WriteBufferManager((2 * 1024  * 1024 * 1023) + 1023, cache);
//    dbOptions.write_buffer_manager.reset(manager);


    // THIS WORKS DEF ->
    // 64 mb def by one column took ~ 30 s
    // 128 mb def by one column took 15
    // 2 gig by 50 column took took 14
    // these numbers above with small keys 32 kb
    // with larger keys there where no diff?
    dbOptions.db_write_buffer_size = 0; // (2 * 1024  * 1024 * 1023) + 1023;
    // necessary otherwise our benchmark fails
    dbOptions.max_open_files = 512;

    const int columnFamilyCount = 50;
    ColumnFamilyOptions columnFamilyOptions = ColumnFamilyOptions();
    columnFamilyOptions.max_write_buffer_number = 1;
    columnFamilyOptions.write_buffer_size = 32 * 1024 * 1024;

    columnFamilyOptions.max_write_buffer_size_to_maintain = 0;
    columnFamilyOptions.min_write_buffer_number_to_merge = 0;
//    columnFamilyOptions.optimize_filters_for_hits = true;
//    columnFamilyOptions.table_factory.reset(NewBlockBasedTableFactory(basedTableOptions));

    std::vector<ColumnFamilyDescriptor> column_families;
    createColumnFamilies(columnFamilyCount, columnFamilyOptions, column_families);

    // open DB
    std::vector<ColumnFamilyHandle *> handles;
    DB *db;
    OptimisticTransactionDB *txn_db;
    Status s = OptimisticTransactionDB::Open(dbOptions, kDBPath, column_families, &handles, &txn_db);
    assert(s.ok());
    db = txn_db->GetBaseDB();

    WriteOptions write_options;
    ReadOptions read_options;
    OptimisticTransactionOptions txn_options;
    std::string value;
    ////////////////////////////////////////////////////////
    //
    // Benchmark
    //
    ////////////////////////////////////////////////////////

    const int transactionCount = 10000;

    std::cout << "~ Start Benchmark ~" << std::endl;
    std::cout << "Column family count: " << columnFamilyCount << std::endl;
    std::cout << "Transaction count: " << transactionCount << std::endl;

    const clock_t startTime = clock();
    auto key = std::string(1024 * 1024, 'a');
    clock_t iterationTime = clock();
    for (int i = 0; i < transactionCount; ++i) {

        // Start a transaction
        Transaction *txn = txn_db->BeginTransaction(write_options);
        assert(txn);

        // Write a key in this transaction
        ColumnFamilyHandle *&family = handles.at(i % handles.size());
        s = txn->Put(family, key + std::to_string(i), "abc");
        assert(s.ok());

        // Commit transaction
        s = txn->Commit();
        if (!s.ok())
        {
            std::cout << s.ToString() << std::endl;
        }

        if (i % 100 == 0)
        {
            const clock_t currentClock = clock();
            std::cout << "Transaction " << i << " took " << double(currentClock - iterationTime) / CLOCKS_PER_SEC << " secs since last iteration." << std::endl;
            iterationTime = currentClock;
        }
        delete txn;
    }


    const clock_t endTime = clock();

    std::cout << "~ Finished OptimisticDB benchmark ~" << std::endl;
    std::cout << " Benchmark took: " << double(endTime - startTime) / CLOCKS_PER_SEC << " sec" << std::endl;

    std::cout << " statistics: " << dbOptions.statistics.get()->ToString() << std::endl;

    // Cleanup
    delete txn_db;
    DestroyDB(kDBPath, Options(), column_families);
    return 0;
}

void createColumnFamilies(const int columnFamilyCount, const ColumnFamilyOptions &columnFamilyOptions,
                          std::vector<ColumnFamilyDescriptor> &column_families) {// have to open default column family
    column_families.push_back(ColumnFamilyDescriptor(
            kDefaultColumnFamilyName, columnFamilyOptions));

    for (int idx = 0; idx < columnFamilyCount; ++idx) {
        std::string name = "cf_" + std::to_string(idx);
        // open the new one, too
        column_families.push_back(ColumnFamilyDescriptor(
                name, columnFamilyOptions));
    }
}

#endif  // ROCKSDB_LITE
