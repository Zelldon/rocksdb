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

using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "/tmp/rocksdb_transaction_example";

void createColumnFamilies(const int columnFamilyCount, const ColumnFamilyOptions &columnFamilyOptions,
                          std::vector<ColumnFamilyDescriptor> &column_families);

int main() {
    DBOptions dbOptions;
    dbOptions.create_if_missing = true;
    dbOptions.create_missing_column_families = true;

    std::cout << "DBOptions: " << std::endl;
    std::cout << "\t" << "db_write_buffer_size: " << dbOptions.db_write_buffer_size << std::endl;
    std::cout << "\t" << "max_open_files: " << dbOptions.max_open_files << std::endl;
    std::cout << "\t" << "write_buffer_manager: " << dbOptions.write_buffer_manager << std::endl;


    const int columnFamilyCount = 50;
    ColumnFamilyOptions columnFamilyOptions = ColumnFamilyOptions();

    std::cout << "ColumnFamilyOptions: " << std::endl;
    std::cout << "\t" << "write_buffer_size: " << columnFamilyOptions.write_buffer_size << std::endl;
    std::cout << "\t" << "arena_block_size: " << columnFamilyOptions.arena_block_size << std::endl;

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

    const int transactionCount = 100000;

    std::cout << "~ Start Benchmark ~" << std::endl;
    std::cout << "Column family count: " << columnFamilyCount << std::endl;
    std::cout << "Transaction count: " << transactionCount << std::endl;

    const clock_t startTime = clock();
    for (int i = 0; i < transactionCount; ++i) {

        // Start a transaction
        Transaction *txn = txn_db->BeginTransaction(write_options);
        assert(txn);

        // Write a key in this transaction
        ColumnFamilyHandle *&family = handles.at(i % handles.size());
        s = txn->Put(family, std::string(1024 * ( (i % 1024) + 1), 'a'), "abc");
        assert(s.ok());

        // Commit transaction
        s = txn->Commit();
        assert(s.ok());
        delete txn;
    }


    const clock_t endTime = clock();

    std::cout << "~ Finished OptimisticDB benchmark ~" << std::endl;
    std::cout << " Benchmark took: " << double(endTime - startTime) / CLOCKS_PER_SEC << " sec" << std::endl;

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
