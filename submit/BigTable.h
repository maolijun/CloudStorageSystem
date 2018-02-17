/*
 * BigTable.h
 *
 *  Created on: Apr 4, 2017
 *      Author: Jian Li
 */

#ifndef BIGTABLE_H_
#define BIGTABLE_H_

#include <iostream>
#include <string.h>
#include <map>
#include <stdio.h>
#include <fstream>
#include <thread>
#include <vector>
#include <unordered_map>
#include <boost/filesystem.hpp>
#include <utility>
#include <tuple>
#include <ctime>
#include <algorithm>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <queue>
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_types.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <mutex>

using namespace std;

#define EMPTY ""

const int MAX_BUFFER_SIZE = 5000000;
const int REMOVAL_GAP = 3600;

class File_data {
public:
	string username;
	string filename;
	string file_type;
	int buff_start;
	int file_length;

	bool is_flushed;
	bool is_deleted;
	string flush_filename;

	File_data(string username, string filename, string file_type, int buff_start, int file_length, bool is_flushed, bool is_deleted, string flush_filename) {
		this->username = username;
		this->filename = filename;
		this->file_type = file_type;
		this->buff_start = buff_start;
		this->file_length = file_length;
		this->is_flushed = is_flushed;
		this->is_deleted = is_deleted;
		this->flush_filename = flush_filename;
	}
};

class BigTable {
	// this server's id
	string  server_id;
	// current file index that we should put the memory content in if exceed the max limit.
	int file_index;
	//map from username to (map from filename to the file metadata.)
	unordered_map<string, unordered_map<string, File_data>> bigtable;

	// the all content in memory.
	//char memtable[MAX_BUFFER_SIZE];
	char* memtable = new char[MAX_BUFFER_SIZE];
	// current position in the memtable.
	int curr_pos;
	// using a vector to store all the (username/filename) in current memtable.
	vector<string> memory_files;
	// map from in disk files name to a vector of all the corresponding (username/filename).
	unordered_map<string, vector<string>> disk_files;
	// map from the file index name to a vector of pair of time and File metadata.
	unordered_map<string, vector<pair<time_t, File_data>>> deleted_files;
	// map from the file index to the boost:: shared_mutex
	unordered_map<string, boost::shared_mutex> file_mutex;
	
public:
	// the mutex for the log file.
	mutex log_mutex;
	// the mutex for each flushed file 
	mutex mem_file_mutex;
	mutex bigtable_file_mutex;
	mutex deleted_file_mutex;
public:
	BigTable();
	BigTable(string server_id);
	BigTable(string s, string memtable_file, string bigtable_file, string deleted_file_name);

	bool setup(string s);
	bool check_file(string username, string filename);
	bool put(string username, string filename, const char file_content[], int file_size, string file_type);
	bool cput(string username, string filename, const char old_file_content[], int old_file_size, const char new_file_content[],
	int new_file_size, string file_type);
	bool delete_file(string username, string filename);
	bool get(string username, string filename, char * res, int size);
	// remove the previously lay deleted files and write these operations into the log file.
	bool remove_from_disk_log(string log_file);
	bool have_file_to_be_deleted(vector<pair<time_t, File_data>> files);
	bool remove_files(auto it_map, auto it_vector, ofstream & ofs);
	// to check whether the username-filename-type exist in the bigtable.
	bool get_folder(string username, string filename, string type);
	bool put_folder(string username, string filename, string type);
	bool delete_folder(string username, string filename, string type);

	bool get_size(string username, string filename, int& size);
	string get_file(string username, string filename);
	bool put_file(string username, string filename, const char file_content[], int file_size, string filetype);
	bool move_file(string username, string oldfilename, string newfilename, string type);
	bool move_folder(string username, string oldfilename, string newfilename, string type);
	
	// the API for checkpointing and recovery.
	// the master should call this method with 2 filenames, no need to include the server_id.
	bool flush_tables(string memtable_file, string bigtable_file, string deleted_file_name);

	bool flush_memtable(string memtable_file);
	bool flush_bigtable(string bigtable_file);
	bool flush_deleted_files(string deleted_file);

	bool load_memtable(string memtable_file);
	bool load_bigtable(string bigtable_file);
	bool load_deleted_files(string deleted_file_name);

	bool clear_directory(string s);

	bool rename_file(string username, string oldfilename, string newfilename);
    void print_bigtable();
};

#endif /* BIGTABLE_H_ */





