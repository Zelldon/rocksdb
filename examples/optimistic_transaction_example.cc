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

int main() {

    DBOptions dbOptions;
    dbOptions.create_if_missing = true;
    dbOptions.create_missing_column_families = true;

    // enable statistics
    dbOptions.statistics = CreateDBStatistics();
    dbOptions.dump_malloc_stats = true;
//    dbOptions.max_total_wal_size = (2 * 1024  * 1024 * 1023) + 1023;

    // mem table

    const double writeBufferRatio = 0.5;
    const uint64_t totalSize = (uint64_t) (4 * 1024 * 1024) * 1024;
    const double highRatio = 0.1;

    const double cacheSize = ((3 - writeBufferRatio) * totalSize) / 3.0f;
    const double writeBufferSize = 2 * totalSize * writeBufferRatio / 3.0f;

//    dbOptions.db_write_buffer_size = (1 * 1024  * 1024 * 1023) + 1023;


    std::cout << "Cache size: " << cacheSize << " write buffer size: " << writeBufferSize << std::endl;
    auto cache = NewLRUCache(cacheSize, -1, false, highRatio);
    WriteBufferManager* manager = new WriteBufferManager(writeBufferSize, cache);
    dbOptions.write_buffer_manager.reset(manager);

    // 64 mb def by one column took ~ 30 s
    // 128 mb def by one column took 15
    // 2 gig by 50 column took took 14
    // these numbers above with small keys 32 kb
    // with larger keys there where no diff?
//    dbOptions.db_write_buffer_size = (1 * 1024  * 1024 * 1023) + 1023;
    // necessary otherwise our benchmark fails
    // maximum open files in this database
    dbOptions.max_open_files = 512;

    const int columnFamilyCount = 50;
    ColumnFamilyOptions columnFamilyOptions = ColumnFamilyOptions();

    // maximum write buffers, before flushing to disk
    columnFamilyOptions.max_write_buffer_number = 1;
    // memtable size per column family
    columnFamilyOptions.write_buffer_size = 32 * 1024 * 1024;
    // maximum maintained bytes, incl buffers which are already flushed
    columnFamilyOptions.max_write_buffer_size_to_maintain = 0;
    // how many buffers need to merged before write to storage
    columnFamilyOptions.min_write_buffer_number_to_merge = 1;
    // Bloom filter will skip the last level
    // makes sense since know normally the key
    columnFamilyOptions.optimize_filters_for_hits = true;
    columnFamilyOptions.arena_block_size = 4096;

    // index and bloom filter
    // table option
    BlockBasedTableOptions basedTableOptions = BlockBasedTableOptions();
    // index block size, default 4 kb
    basedTableOptions.block_size = 32  * 1024;
    // block cache which is used on reads
    basedTableOptions.block_cache = cache;
    // index, filter should be put in cache
    basedTableOptions.cache_index_and_filter_blocks = true;
    basedTableOptions.cache_index_and_filter_blocks_with_high_priority = false;
    // directly link level 0 in cache
    basedTableOptions.pin_l0_filter_and_index_blocks_in_cache = true;

    // sets index, filter and cache cfg in column setting
    columnFamilyOptions.table_factory.reset(NewBlockBasedTableFactory(basedTableOptions));
//    columnFamilyOptions.arena_block_size = 128 * 1024 * 1024;

    std::vector<ColumnFamilyDescriptor> column_families;
    createColumnFamilies(columnFamilyCount, columnFamilyOptions, column_families);

    // open DB
    std::vector<ColumnFamilyHandle *> handles;
    DB *db;
//    OptimisticTransactionDB *txn_db;


    Status s = DB::Open(dbOptions, kDBPath, column_families, &handles, &db);// OptimisticTransactionDB::Open(dbOptions, kDBPath, column_families, &handles, &txn_db);
    assert(s.ok());
//    db = txn_db->GetBaseDB();

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

//        writeBatch(db, iterationTime, key, handles, i);

//        runTransaction(txn_db, iterationTime, key, handles, i);
    }

    const clock_t endTime = clock();

    std::cout << "~ Finished OptimisticDB benchmark ~" << std::endl;
    std::cout << " Benchmark took: " << double(endTime - startTime) / CLOCKS_PER_SEC << " sec" << std::endl;

    std::cout << " statistics: " << dbOptions.statistics.get()->ToString() << std::endl;


    const std::string currMemTableSize = "rocksdb.cur-size-all-mem-tables";
    const std::string blockCacheUsage = "rocksdb.block-cache-usage";

    for (int i = 0; i < (int) handles.size(); i++) {
        std::string value;
        db->GetProperty(handles.at(i), currMemTableSize, &value);
        std::cout << handles.at(i)->GetName() << " mem " << value << std::endl;
        db->GetProperty(handles.at(i), blockCacheUsage, &value);
        std::cout << handles.at(i)->GetName() << " block cache usage " << value << std::endl;
    }

    // Cleanup
//    delete txn_db;
    delete db;
    DestroyDB(kDBPath, Options(), column_families);
    return 0;
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
