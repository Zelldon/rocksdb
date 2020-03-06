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

int main() {
    // open DB
    BlockBasedTableOptions table_options;
    table_options.block_cache = NewLRUCache(1 * 1024 * 1024 * 1024L);

    DBOptions options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    //    options.create_if_missing = true;
//    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    DB *db;
    OptimisticTransactionDB *txn_db;


    const int columnFamilyCount = 100;

    std::vector<ColumnFamilyDescriptor> column_families;
    // have to open default column family
    column_families.push_back(ColumnFamilyDescriptor(
            kDefaultColumnFamilyName, ColumnFamilyOptions()));
    for (int idx = 0; idx < columnFamilyCount; ++idx) {

        std::string name = "cf_" + std::to_string(idx);

        std::cout << name << std::endl;
        // open the new one, too
        column_families.push_back(ColumnFamilyDescriptor(
                name, ColumnFamilyOptions()));
    }

    std::vector<ColumnFamilyHandle*> handles;
    Status s = OptimisticTransactionDB::Open(options, kDBPath, column_families, &handles, &txn_db);

//    std::cout << s.code() << " " << s.getState() << std::endl;
    assert(s.ok());
    db = txn_db->GetBaseDB();

    WriteOptions write_options;
    ReadOptions read_options;
    OptimisticTransactionOptions txn_options;
    std::string value;


    ////////////////////////////////////////////////////////
    //
    // Simple OptimisticTransaction Example ("Read Committed")
    //
    ////////////////////////////////////////////////////////

    const int transactionCount = 100000;
    for (int i = 0; i < transactionCount; ++i) {

        // Start a transaction
        Transaction *txn = txn_db->BeginTransaction(write_options);
        assert(txn);

        // Write a key in this transaction

        ColumnFamilyHandle *&family = handles.at(i % handles.size());
        s = txn->Put(family, "abc", std::string(1024 * 1024, 'a'));
        assert(s.ok());

        std::cout << "Iter: " << i;
        std::cout << " column family: " << family->GetName() << " used block cache: " << table_options.block_cache->GetUsage();

        std::string out;
        db->GetProperty("rocksdb.estimate-table-readers-mem", &out);
        std::cout << "estimated table readers mem: " << out;

        db->GetProperty("rocksdb.cur-size-all-mem-tables", &out);
        std::cout << "cur size all mem tables: " << out << std::endl;

        // Commit transaction
        s = txn->Commit();
        delete txn;
    }

    std::cout << "Out side the transaction used: " << table_options.block_cache->GetUsage() << std::endl;


    // Cleanup
//  delete txn_db;
//  DestroyDB(kDBPath, options);
    return 0;
}

#endif  // ROCKSDB_LITE
