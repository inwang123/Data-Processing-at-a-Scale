// General Libraries
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include "csv.hpp"

// RocksDB Libraries
#include <rocksdb/db.h>
#include <rocksdb/options.h>


// Namespaces
using namespace std;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Iterator;


// Function to create a kvs
DB* create_kvs(const string& csv_file_path, const string& db_path) {

    // Open the csv file
    csv::CSVReader reader(csv_file_path);
    csv::CSVRow row;

    // Get the headers
    vector<string> header = reader.get_col_names();

    #if 0
        /* Something that can help figure out some of the parsing through the data
        To print, you need to implement the main function and add a new rule to the Makefile
        However, just going over the general logic of this code block should be helpful 
        in addition to the documentation of csv.hpp */
        int col_no = 0;
        for (csv::CSVRow& row : reader) {
            col_no = 0;
            cout << "ID: " << row["col_name"] << endl;
            for (csv::CSVField& field : row) {
                cout << header[col_no]  << ": " << field.get<string>() << endl;
                col_no++;
            }
            break;
        }
    #endif

    DB* db;

    // TODO: Open RocksDB database with options
    Options options;
    options.create_if_missing = true;
    Status s = DB::Open(options, db_path, &db);
    
    if (!s.ok()) {
        cerr << "Failed to open database: " << s.ToString() << endl;
        return nullptr;
    }

    // TODO: Load CSV data into database using WriteBatch
    WriteBatch batch;
    int row_count = 0;
    
    for (csv::CSVRow& row : reader) {
        string id_value;
        
        // get id val of row
        for (size_t i = 0; i < header.size(); i++) {
            if (header[i] == "id") {
                id_value = row[i].get<string>();
                break;
            }
        }
        
        // key-value pairs per column
        for (size_t i = 0; i < header.size(); i++) {
            string key = id_value + "_" + header[i];
            string value = row[i].get<string>();
            batch.Put(key, value);
        }
        
        row_count++;
        
        // batches here of 1500 rows
        if (row_count % 1500 == 0) {
            WriteOptions write_options;
            write_options.sync = false;
            s = db->Write(write_options, &batch);
            if (!s.ok()) {
                cerr << "Batch write failed: " << s.ToString() << endl;
            }
            batch.Clear();
        }
    }
    
    // Write remaining batch
    if (row_count % 1500 != 0) {
        WriteOptions write_options;
        write_options.sync = false;
        s = db->Write(write_options, &batch);
        if (!s.ok()) {
            cerr << "Final batch write failed: " << s.ToString() << endl;
        }
    }

    return db;
}


// Function to perform a MultiGet operation
vector<string> multi_get(DB* db, const vector<string>& keys) {
    vector<string> values;

    // TODO: Implement MultiGet operation
    if (!db || keys.empty()) {
        return values;
    }
    
    // strings -> slice objs
    vector<Slice> key_slices;
    for (const auto& key : keys) {
        key_slices.push_back(Slice(key));
    }
    
    vector<Status> statuses;
    vector<string> results;
    
    // multiGet operation here
    statuses = db->MultiGet(ReadOptions(), key_slices, &results);
    
    //filter display_name vals+check status
    for (size_t i = 0; i < results.size(); i++) {
        if (statuses[i].ok()) {
            // Only include if it's a display_name key
            if (keys[i].find("_display_name") != string::npos) {
                values.push_back(results[i]);
            }
        }
    }

    // Only return the display_name of the subreddit(s)
    return values;
}

// Function to iterate over a range of keys and return the corresponding values
vector<string> iterate_over_range(DB* db, const string& start_key, const string& end_key) {
    vector<string> result;

    // TODO: Create iterator and iterate from start_key to end_key
    if (!db) {
        return result;
    }
    
    ReadOptions read_options;
    unique_ptr<Iterator> it(db->NewIterator(read_options));
    
    // Start from the beginning of the range
    it->Seek(start_key);
    
    while (it->Valid()) {
        string current_key = it->key().ToString();
        
        // make sure it doesnt exceed end of range
        if (current_key > end_key) {
            break;
        }
        
        // TODO: Filter results to only include keys containing "_display_name"
        if (current_key.find("_display_name") != string::npos) {
            result.push_back(it->value().ToString());
        }
        
        it->Next();
    }
    
    // Check for any errors
    if (!it->status().ok()) {
        cerr << "Iterator error: " << it->status().ToString() << endl;
    }

    // Only return the display_name of the subreddit(s)
    return result;
}

// Function to delete a particular comment from the kvs
Status delete_key(DB* db, const string& key) {
    Status s;

    // TODO: Delete the key from the database
    if (!db) {
        return Status::InvalidArgument("Database is null");
    }
    
    s = db->Delete(WriteOptions(), key);

    return s;
}