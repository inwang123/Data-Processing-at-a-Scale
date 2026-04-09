// General Libraries
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
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

    // TODO: Load CSV data into database using WriteBatch

    return db;
}


// Function to perform a MultiGet operation
vector<string> multi_get(DB* db, const vector<string>& keys) {
    vector<string> values;

    // TODO: Implement MultiGet operation

    // Only return the display_name of the subreddit(s)
    return values;
}

// Function to iterate over a range of keys and return the corresponding values
vector<string> iterate_over_range(DB* db, const string& start_key, const string& end_key) {
    vector<string> result;

    // TODO: Create iterator and iterate from start_key to end_key
    // TODO: Filter results to only include keys containing "_display_name"

    // Only return the display_name of the subreddit(s)
    return result;
}

// Function to delete a particular comment from the kvs
Status delete_key(DB* db, const string& key) {
    Status s;

    // TODO: Delete the key from the database

    return s;
}
