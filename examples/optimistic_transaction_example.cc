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


void runTransaction(OptimisticTransactionDB* txn_db,
                    clock_t& iterationTime,
                    std::string& key,
                    std::vector<ColumnFamilyHandle *> handles,
                    int i);


void writeBatch(DB* db,
                clock_t& iterationTime,
                std::string& key,
                std::vector<ColumnFamilyHandle *> handles,
                int i);


void printProperty(DB* db, ColumnFamilyHandle *&pColumnFamilyHandle, const std::string &property);

void printColumnFamilyStats(std::vector<ColumnFamilyHandle *> &handles, DB *db, int &totalCurSizeAllMemTables,
                            int &totalCurSizeActiveMemTable, int &totalSizeAllMemTables);

int main() {

    DBOptions dbOptions;
    dbOptions.create_if_missing = true;
    dbOptions.create_missing_column_families = true;


    // enable statistics
    dbOptions.statistics = CreateDBStatistics();
    dbOptions.dump_malloc_stats = true;
//    dbOptions.max_total_wal_size = (2 * 1024  * 1024 * 1023) + 1023;

    // mem table

    // calculation reused from flink
    const double writeBufferRatio = 0.5;
    const uint64_t totalSize = (uint64_t) (2 * 1024 * 1024) * 1024;
    const double highRatio = 0.1;

    const double cacheSize = ((3 - writeBufferRatio) * totalSize) / 3.0f;
    const double writeBufferSize = 2 * totalSize * writeBufferRatio / 3.0f;

    std::cout << "Cache size: " << cacheSize << " write buffer size: " << writeBufferSize << std::endl;
    auto cache = NewLRUCache(cacheSize, -1, true, highRatio);
    // manager supervise write buffer size
    auto* manager = new WriteBufferManager(writeBufferSize, cache);
    dbOptions.write_buffer_manager.reset(manager);

    dbOptions.create_if_missing = true;
    dbOptions.statistics = CreateDBStatistics();
    dbOptions.max_open_files = 100;

    dbOptions.paranoid_checks

    const int columnFamilyCount = 50;
    ColumnFamilyOptions columnFamilyOptions = ColumnFamilyOptions();

    // RSS will increase if we have more files on disk
    columnFamilyOptions.write_buffer_size = 8 * 1024 * 1024;
    columnFamilyOptions.max_write_buffer_number = 2;

//    columnFamilyOptions.min_write_buffer_number_to_merge = 1;

    columnFamilyOptions.arena_block_size = 4 * 1024;
//    columnFamilyOptions.optimize_filters_for_hits = true;
//    columnFamilyOptions.target_file_size_base = 256 * 1024 * 1024;

    // index and bloom filter
    // table option
    BlockBasedTableOptions basedTableOptions = BlockBasedTableOptions();

    basedTableOptions.block_size = 32  * 1024;
    basedTableOptions.block_cache = cache;
    basedTableOptions.cache_index_and_filter_blocks = false;
    basedTableOptions.cache_index_and_filter_blocks_with_high_priority = false;
    // directly link level 0 in cache
    basedTableOptions.pin_l0_filter_and_index_blocks_in_cache = false;

    // sets index, filter and cache cfg in column setting
    columnFamilyOptions.table_factory.reset(NewBlockBasedTableFactory(basedTableOptions));

    std::vector<ColumnFamilyDescriptor> column_families;
    createColumnFamilies(columnFamilyCount, columnFamilyOptions, column_families);

    // open DB
    std::vector<ColumnFamilyHandle *> handles;
    DB *db;
    Status s = DB::Open(dbOptions, kDBPath, column_families, &handles, &db);
    // OptimisticTransactionDB::Open(dbOptions, kDBPath, column_families, &handles, &txn_db);
    assert(s.ok());

    ////////////////////////////////////////////////////////
    //
    // Benchmark
    //
    ////////////////////////////////////////////////////////
    const int transactionCount = 1000 * 32;

    std::cout << "~ Start Benchmark ~" << db->GetName() << std::endl;
    std::cout << "Column family count: " << columnFamilyCount << std::endl;
    std::cout << "Transaction count: " << transactionCount << std::endl;

    const clock_t startTime = clock();
    auto key = std::string(32 * 1024, 'a');
    clock_t iterationTime = clock();
    for (int i = 0; i < transactionCount; ++i) {

        auto newKey = key + std::to_string(i);
        WriteOptions write_options;

        // Write a key in this transaction
        ColumnFamilyHandle *&family = handles.at(i % handles.size());
        s = db->Put(write_options, family, newKey, "abc");

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

//        if (i % 1000)
//        {
//            int totalCurSizeAllMemTables = 0;
//            int totalCurSizeActiveMemTable = 0;
//            int totalSizeAllMemTables = 0;
//
//            printColumnFamilyStats(handles, db, totalCurSizeAllMemTables, totalCurSizeActiveMemTable, totalSizeAllMemTables);
//
//            std::cout << "totalCurSizeAllMemTables: " << ((totalCurSizeAllMemTables / 1024.0f) / 1024.0f) << " MB" << std::endl;
//            std::cout << "totalCurSizeActiveMemTable: " << ((totalCurSizeActiveMemTable / 1024.0f) / 1024.0f) << " MB" << std::endl;
//            std::cout << "totalSizeAllMemTables: " << ((totalSizeAllMemTables / 1024.0f) / 1024.0f) << " MB" << std::endl;
//        }
    }

    const clock_t endTime = clock();

    std::cout << "~ Finished OptimisticDB benchmark ~" << std::endl;
    std::cout << " Benchmark took: " << double(endTime - startTime) / CLOCKS_PER_SEC << " sec" << std::endl;

//    std::cout << " statistics: " << dbOptions.statistics.get()->ToString() << std::endl;


    const std::string currMemTableSize = "rocksdb.cur-size-all-mem-tables";
    const std::string blockCacheUsage = "rocksdb.block-cache-usage";


    int totalCurSizeAllMemTables = 0;
    int totalCurSizeActiveMemTable = 0;
    int totalSizeAllMemTables = 0;

    printColumnFamilyStats(handles, db, totalCurSizeAllMemTables, totalCurSizeActiveMemTable, totalSizeAllMemTables);

    std::cout << "totalCurSizeAllMemTables: " << ((totalCurSizeAllMemTables / 1024.0f) / 1024.0f) << " MB" << std::endl;
    std::cout << "totalCurSizeActiveMemTable: " << ((totalCurSizeActiveMemTable / 1024.0f) / 1024.0f) << " MB" << std::endl;
    std::cout << "totalSizeAllMemTables: " << ((totalSizeAllMemTables / 1024.0f) / 1024.0f) << " MB" << std::endl;
    printProperty(db, handles.at(0), rocksdb::DB::Properties::kBlockCacheUsage);
    printProperty(db, handles.at(0), rocksdb::DB::Properties::kBlockCachePinnedUsage);
    std::cout << "Expected total size: " << totalSize << std::endl;


    printProperty(db, handles.at(0), rocksdb::DB::Properties::kEstimateTableReadersMem);


    // Cleanup
//    delete txn_db;
    delete db;
    DestroyDB(kDBPath, Options(), column_families);
    return 0;
}

void printColumnFamilyStats(std::vector<ColumnFamilyHandle *> &handles, DB *db, int &totalCurSizeAllMemTables,
                            int &totalCurSizeActiveMemTable, int &totalSizeAllMemTables) {
    for (int i = 0; i < (int) handles.size(); i++) {
        ColumnFamilyHandle *&pColumnFamilyHandle = handles.at(i);
        std::cout << "ColumnFamily: " << pColumnFamilyHandle->GetName() << std::endl;


        std::string value;
        db->GetProperty(pColumnFamilyHandle, DB::Properties::kCurSizeAllMemTables, &value);
        totalCurSizeAllMemTables += std::stoi(value);

        db->GetProperty(pColumnFamilyHandle, DB::Properties::kCurSizeActiveMemTable, &value);
        totalCurSizeActiveMemTable += std::stoi(value);

        db->GetProperty(pColumnFamilyHandle, DB::Properties::kSizeAllMemTables, &value);
        totalSizeAllMemTables += std::stoi(value);


        printProperty(db, pColumnFamilyHandle, DB::Properties::kCurSizeAllMemTables);
        printProperty(db, pColumnFamilyHandle, DB::Properties::kCurSizeActiveMemTable);
        printProperty(db, pColumnFamilyHandle, DB::Properties::kSizeAllMemTables);
        printProperty(db, pColumnFamilyHandle, DB::Properties::kBlockCacheUsage);
        printProperty(db, pColumnFamilyHandle, DB::Properties::kBlockCacheCapacity);
        printProperty(db, pColumnFamilyHandle, DB::Properties::kBlockCachePinnedUsage);
        printProperty(db, pColumnFamilyHandle, DB::Properties::kEstimateNumKeys);

        printProperty(db, handles.at(0), DB::Properties::kEstimateTableReadersMem);
    }
}

void printProperty(DB* db, ColumnFamilyHandle *&pColumnFamilyHandle, const std::string &property) {
    std::string value;
    db->GetProperty(pColumnFamilyHandle, property, &value);
    std::cout << property << ": " << value << std::endl;
}

void writeBatch(DB* db,
                clock_t& iterationTime,
                std::string& key,
                std::vector<ColumnFamilyHandle *> handles,
                int i)
{

    WriteOptions write_options;
    auto newKey = key + std::to_string(i);

    WriteBatch writeBatch = WriteBatch();
    // Write a key in this transaction
    ColumnFamilyHandle *&family = handles.at(i % handles.size());

    Status s = writeBatch.Put(family, newKey, "abc");
    assert(s.ok());


    s = db->Write(write_options, &writeBatch);
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
}

void runTransaction(OptimisticTransactionDB* txn_db,
                    clock_t& iterationTime,
                    std::string& key,
                    std::vector<ColumnFamilyHandle *> handles,
                    int i)
{
    WriteOptions write_options;
    ReadOptions read_options;
    OptimisticTransactionOptions txn_options;
    std::string value;

//    write_options.disableWAL = true;
//    write_options.memtable_insert_hint_per_batch = true;
    auto newKey = key + std::to_string(i);

    // Start a transaction
    Transaction *txn = txn_db->BeginTransaction(write_options);
    assert(txn);

    txn->DisableIndexing();
    // Write a key in this transaction
    ColumnFamilyHandle *&family = handles.at(i % handles.size());

    Status s = txn->Put(family, newKey, "abc");
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
