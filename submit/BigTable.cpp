/*
 * BigTable.cpp
 *
 *  Created on: Apr 4, 2017
 *      Author: Jian Li
 */

#include "BigTable.h"

using namespace std;

BigTable::BigTable() {
	cout << "New bigtable" << endl;
};

BigTable::BigTable(string s) {
	clear_directory(s);

	boost::filesystem::path dir(s);
	if (!boost::filesystem::exists(dir)) {
		if (boost::filesystem::create_directory(dir)) {
			cout << "Create a new server directory " << s << endl;
		} else {
			cout << "Failed to create the server directory " << s << endl;
		}
	} else {
		cout << "Server directory " << s << " already exists" << endl;
	}
	server_id = s;
	curr_pos = 0;
	file_index = 0;
	deleted_files.emplace(piecewise_construct, forward_as_tuple(to_string(file_index)), forward_as_tuple());
}

BigTable::BigTable(string s, string memtable_file, string bigtable_file, string deleted_file_name) {
	server_id = s;
	load_memtable(s + "/" + memtable_file);
	// update current file_index when loading the bigtable.
	load_bigtable(s + "/" + bigtable_file);
	load_deleted_files(s + "/" + deleted_file_name);
}


bool BigTable::setup(string s) {

	boost::filesystem::path dir(s);
	if (!boost::filesystem::exists(dir)) {
		if (boost::filesystem::create_directory(dir)) {
			cout << "Create a new server directory " << s << endl;
		} else {
			cout << "Failed to create the server directory " << s << endl;
		}
	} else {
		cout << "Server directory " << s << " already exists" << endl;
	}
	server_id = s;
	curr_pos = 0;
	file_index = 0;
	deleted_files.emplace(piecewise_construct, forward_as_tuple(to_string(file_index)), forward_as_tuple());
}

bool BigTable::check_file(string username, string filename) {
    cout << "check_file\n";
	if (bigtable.find(username) == bigtable.end() || bigtable[username].find(filename) == bigtable[username].end())
		return false;
	return true;
}

// Put the file content corresponding to the username and filename.
bool BigTable::put(string username, string filename, const char file_content[], int file_size, string file_type) {
	// first check whether there is a file with this file name
	// if not
    cout << "put\n";
	if (bigtable.find(username) == bigtable.end() || bigtable[username].find(filename) == bigtable[username].end() ||
			bigtable.at(username).at(filename).is_deleted) {
        cout << "put1\n";
		if (bigtable.find(username) == bigtable.end()) {
			bigtable.emplace(piecewise_construct, forward_as_tuple(username), forward_as_tuple());
		}
		if (bigtable.find(username) != bigtable.end() && bigtable[username].find(filename) != bigtable[username].end()) {
			bigtable.at(username).erase(filename);
		}
		File_data temp(username, filename, file_type, curr_pos, file_size, false, false, to_string(file_index));
		bigtable.at(username).emplace(filename, temp);
	} else {
		// the file is already exist and not marked deleted.
		return false;
	}
	// if not reach the limit, still write to the memtable.
	if (curr_pos + file_size < MAX_BUFFER_SIZE) {
        cout << "put2\n";
		for (int i = 0; i < file_size; i++) {
			memtable[curr_pos++] = file_content[i];
		}

		string file_temp_name = username + "/" + filename;
		memory_files.push_back(file_temp_name);
		return true;
	} else {
        cout << "put3\n";
		// else we need to write the corresponding content into the file.
		string file_path_name = server_id + "/" + to_string(file_index);
		ofstream myfile(file_path_name);
		if (!myfile.is_open()) {
			cerr << "Cannot open the file: " << file_path_name << endl;
			return false;
		}
        cout << "put4\n";
		myfile.write((char *)&memtable[0], curr_pos);
		myfile.write((char *)&file_content[0], file_size);
		myfile.close();

		// mark it as flushed.
		string file_temp_name = username + "/" + filename;
		memory_files.push_back(file_temp_name);
        cout << "put5\n";
		for (int i = 0; i < memory_files.size(); i++) {
			string temp = memory_files[i];
			size_t pos = temp.find("/");
			string temp_username = temp.substr(0, pos);
			string temp_filename = temp.substr(pos + 1);
			bigtable.at(temp_username).at(temp_filename).is_flushed = true;
		}
        cout << "put6\n";
		// add new mutex for this file
		file_mutex.emplace(piecewise_construct, forward_as_tuple(to_string(file_index)), forward_as_tuple());

		disk_files.emplace(to_string(file_index), memory_files);
		memory_files.clear();
        cout << "put7\n";
		// clear the buffer
		memset(memtable, 0, curr_pos);
		curr_pos = 0;
		file_index++;
		//
		deleted_files.emplace(piecewise_construct, forward_as_tuple(to_string(file_index)), forward_as_tuple());
        cout << "put8\n";
		return true;
	}
}

bool BigTable::get_size(string username, string filename, int& size){
    cout << "get_size\n";
	if (bigtable.find(username) == bigtable.end() || bigtable[username].find(filename) == bigtable[username].end()) {
		return false;
	}
	File_data data = bigtable.at(username).at(filename);
	if (data.is_deleted)
		return false;
	size = data.file_length;
	return true;
}

bool BigTable::get(string username, string filename, char* res, int res_size) {
    cout << "get\n";
	if (bigtable.find(username) == bigtable.end() || bigtable[username].find(filename) == bigtable[username].end()) {
		return false;
	}
    cout << "get1\n";
	// check if deleted
	File_data data = bigtable.at(username).at(filename);
	if (data.is_deleted) return false;
    cout << "get2\n";
	// if not, check whether it is flushed
	// if so, read from files
	if (data.is_flushed) {
        cout << "get3\n";
		// get which file it is in
		string file_path = server_id + "/" + data.flush_filename;

		// lock the corresponding file using shared lock for multiple reader.
		// sharable read lock.
		boost::shared_lock<boost::shared_mutex> lock(file_mutex.at(data.flush_filename));
        cout << "get4\n";
		int fd;
		struct stat sb;
		off_t pa_offset;
		fd = open(file_path.c_str(), O_RDONLY);
		if (fd == -1) {
			cerr << "Fail to open" << endl;
			return false;
		}
        cout << "get5\n";
		// to obtain file size
		if (fstat(fd, &sb) == -1) {
			cerr << "Fail to get the status" << endl;
			return false;
		}
        cout << "get6\n";
		int offset = data.buff_start;
		pa_offset = offset & ~(sysconf(_SC_PAGE_SIZE) - 1); /* offset for mmap() must be page aligned */
		if (offset >= sb.st_size) {
			cerr << "offset is past end of file" << endl;
			return false;
		}
        cout << "get7\n";
		int length = data.file_length;
		if (offset + length > sb.st_size) {
			length = sb.st_size - offset;
		}
        cout << "get8\n";
		char * addr;
		addr = (char *) mmap(NULL, length + offset - pa_offset, PROT_READ, MAP_PRIVATE, fd, pa_offset);
		if (addr == MAP_FAILED) {
			cerr << "mmap fail" << endl;
			return false;
		}
        cout << "get9\n";
		int len = data.file_length;
		if (res_size < data.file_length) len = res_size;

		for (int i = 0; i < len; i++) {
			res[i] = *(addr + offset - pa_offset + i);
		}
		res[len] = 0;
		// unmap and close the fd
		munmap(addr, offset + length - pa_offset);
		close(fd);
        cout << "get10\n";
		return true;

	} else {
        cout << "get11\n";
		// if not, read from the memtable array.
		int num = data.file_length;
		if (res_size < num) {
			num = res_size;
		}
		int start = data.buff_start;
		for (int i = 0; i < num; i++) {
			res[i] = memtable[start + i];
		}
		return true;
	}


}

string BigTable::get_file(string username, string filename){
    cout << "get_file\n";
	int file_size;
	string fileContent;
	if(get_size(username, filename, file_size)){
		//std::cout << file_size << "  ";
		//char file_buffer[file_size+1];
		char* file_buffer = new char[file_size+1];

		if(get(username, filename, file_buffer, file_size)){
            cout << "get_file1\n" << std::flush;
			file_buffer[file_size] = '\0';
			std::cout << file_buffer << std::flush;
			fileContent = file_buffer;
			delete file_buffer;
			return fileContent;
		}
		else{
            cout << "get_file2\n" << std::flush;
			delete file_buffer;
			return EMPTY;
		}
	}
	else
		return EMPTY;
}

bool BigTable::cput(string username, string filename, const char old_file_content[], int old_file_size, const char new_file_content[],
		int new_file_size, string file_type) {
	// if the file does not exist.
    cout << "cput\n";
	if (bigtable.find(username) == bigtable.end() || bigtable[username].find(filename) == bigtable[username].end()) {
		return false;
	}
    cout << "cput1\n";
	// get the old file content by getting
	if (old_file_size != bigtable.at(username).at(filename).file_length) return false;
	cout << "cput2\n";
	char* temp_buffer = new char[old_file_size+1];
	get(username, filename, temp_buffer, old_file_size);
	for (int i = 0; i < old_file_size; i++) {
		if (temp_buffer[i] != old_file_content[i]) {
			delete temp_buffer;
			return false;
		}
	}
	cout << "cput3\n";
	// mark the old one deleted.
	delete_file(username, filename);
	// put the new file into the table.
	delete temp_buffer;
	cout << "cput4\n";
	return put(username, filename, new_file_content, new_file_size, file_type);

}

bool BigTable::put_file(string username, string filename, const char file_content[], int file_size, string file_type){
    cout << "put_file\n";
	string old_file_content = get_file(username, filename);
	if(old_file_content.compare(EMPTY) == 0){
        cout << "put_file1\n";
		return put(username, filename, file_content, file_size, file_type);
	}
	else{
        cout << "put_file2\n";
		int old_file_size = old_file_content.length();
		return cput(username, filename, old_file_content.c_str(), old_file_size, file_content, file_size, file_type);
	}
}

bool BigTable::move_file(string username, string old_filename, string new_filename, string type){
    cout << "move_file\n";
	string file_content = get_file(username, old_filename);
	cout << file_content << std::endl;
	if(file_content.compare(EMPTY) != 0){
        cout << "move_file1\n";
		return (put_file(username, new_filename, file_content.c_str(), file_content.length(), type) && delete_file(username, old_filename));
	}
	else
		return false;
}

bool BigTable::delete_file(string username, string filename) {
    cout << "delete_file\n";
	// if this username + filename does not exist.
	if (bigtable.find(username) == bigtable.end() || bigtable[username].find(filename) == bigtable[username].end()) {
		return false;
	}
    cout << "delete_file1\n";
	// if is already deleted, return
	if (bigtable.at(username).at(filename).is_deleted) return false;
	// if not, mark it as deleted.
	bigtable.at(username).at(filename).is_deleted = true;
	string flush_name = bigtable.at(username).at(filename).flush_filename;
	// put into the delete files map
    cout << "delete_file2\n";
	time_t timer;
	time(&timer);
	deleted_files.at(flush_name).push_back(make_pair(timer, bigtable.at(username).at(filename)));
    cout << "delete_file3\n";
	bigtable[username].erase(filename);
    cout << "delete_file4\n";
	return true;
}

struct pred {
	bool operator() (pair<time_t, File_data> pair) {
		time_t timer;
		time(&timer);
		return timer - pair.first > REMOVAL_GAP;
	}
};

bool BigTable::remove_from_disk_log(string log_file) {
	for (auto it = deleted_files.begin(); it != deleted_files.end(); ++it) {
		string curr_file = it->first;
		int curr_file_indx = atoi(curr_file.c_str());
		// just delete the one already write into the disk.
		if (curr_file_indx != file_index) {
			if (have_file_to_be_deleted(it->second)) {

				// because we need to make modifications to the file, then we should lock the file exclusively.
				boost::upgrade_lock<boost::shared_mutex> lock(file_mutex.at(curr_file));
				boost::upgrade_to_unique_lock<boost::shared_mutex> uniqueLock(lock);

				// get the mutex of the log file
				log_mutex.lock();
				// write to the log
				ofstream ofs;
				ofs.open(log_file, ofstream::app);
				string operation = "Delete: " + curr_file + "," ;

				ofs.write(operation.c_str(), operation.size());

				// then replace all the elements in the vector that should be removed.
				auto first_deleted = remove_if(it->second.begin(), it->second.end(), pred());
				// then remove the corresponding files.
				// if success, we also should remove it from the deleted_files.
				if (remove_files(it, first_deleted, ofs)) {
					it->second.erase(first_deleted, it->second.end());
				}
				string line = "\n";
				ofs.write(line.c_str(), line.size());
				ofs.close();
				// unlock the ofs
				log_mutex.unlock();
			}
		}

	}
	return true;
}

class Compare {
public:
	bool operator() (pair<int, string> first, pair<int, string> second) {
		return first.first > second.first;
	}
};

bool BigTable::remove_files(auto it_map, auto it_vector, ofstream & ofs) {
	// create a min heap sorted by the start line in the ascending order.
	priority_queue< pair<int, string>, vector< pair<int, string> >, Compare> pq;
	for (auto temp = it_vector; temp != it_map->second.end(); ++temp) {
		int start_temp = temp->second.buff_start;
		string user_temp = temp->second.username;
		string file_temp = temp->second.filename;

		pq.push(make_pair(start_temp, user_temp + "/" + file_temp));
	}

	// then get the size of the corresponding file
	string file_indx_temp = it_map->first;
	string file_path = server_id + "/" + file_indx_temp;
	ifstream myfile(file_path, ifstream::binary);
	if (!myfile.is_open()) {
		cerr << "Read file cannot open" << endl;
		return false;
	}

	// get length of file:
	myfile.seekg (0, myfile.end);
	int length = myfile.tellg();
	myfile.seekg (0, myfile.beg);

	char * buffer = new char[length +1];
	myfile.read (buffer,length);
	myfile.close();

	// create a temporary file first and then rename
	string temp_filename = file_path + "2";
	ofstream outfile(temp_filename);
	if (!outfile.is_open()) {
		cerr << "Write ile cannot open" << endl;
		return false;
	}

	int new_ptr = 0;
	for (int i = 0; i < disk_files.at(file_indx_temp).size(); i++) {
		string curr = disk_files.at(file_indx_temp)[i];
		// if curr is one of the file that we should delete.
		if (curr == pq.top().second) {
			pq.pop();
			string log = curr + ",";
			ofs.write(log.c_str(), log.size());
			disk_files.at(file_indx_temp).erase(disk_files.at(file_indx_temp).begin() + i);
			i--;
		} else {
			// if not, we should write it to the outfile.
			size_t pos = curr.find("/");
			string user_temp = curr.substr(0, pos);
			string file_temp = curr.substr(pos + 1);
			int start_temp = bigtable.at(user_temp).at(file_temp).buff_start;
			int len = bigtable.at(user_temp).at(file_temp).file_length;
			outfile.write(&buffer[start_temp], len);

			bigtable.at(user_temp).at(file_temp).buff_start = new_ptr;
			new_ptr += len;
		}
	}



	outfile.close();
	delete[] buffer;

	// remove old file and rename new file
	remove(file_path.c_str());
	rename(temp_filename.c_str(), file_path.c_str());

	return true;

}

bool BigTable::have_file_to_be_deleted(vector<pair<time_t, File_data>> files) {
	time_t timer;
	time(&timer);
	for (int i = 0; i < files.size(); i++) {
		int temp_time = files[i].first;
		if (timer - temp_time > REMOVAL_GAP) {
			return true;
		}
	}
	return false;
}


bool BigTable::get_folder(string username, string filename, string type) {
	if (bigtable.find(username) == bigtable.end() || bigtable[username].find(filename) == bigtable[username].end()) {
		return false;
	}

	if (bigtable.at(username).at(filename).file_type != type) {
		return false;
	}
	return true;
}

bool BigTable::put_folder(string username, string filename, string type) {
	// if we already have the correaponding type. return false;
	if (bigtable.find(username) != bigtable.end() && bigtable[username].find(filename) != bigtable[username].end() && bigtable.at(username).at(filename).file_type == type) {
		return false;
	}
	// then if there is no username in the table.
	if (bigtable.find(username) == bigtable.end()) {
		bigtable.emplace(piecewise_construct, forward_as_tuple(username), forward_as_tuple());
	}

	File_data temp(username, filename, type, 0, 0, false, false, "");
	bigtable[username].emplace(filename, temp);
	return true;
}

bool BigTable::delete_folder(string username, string filename, string type) {
	// if the username-filename-type does not exist.
	if (bigtable.find(username) == bigtable.end() || bigtable[username].find(filename) == bigtable[username].end()
			|| bigtable.at(username).at(filename).file_type != type) return false;

	bigtable[username].erase(filename);
	if (bigtable[username].empty()) {
		bigtable.erase(username);
	}
	return true;
}

bool BigTable::move_folder(string username, string oldfilename, string newfilename, string type){
	if(type.compare("folder") != 0)
		return false;
	if(get_folder(username, oldfilename, type)){
		return put_folder(username, newfilename, type) && delete_folder(username, oldfilename, type);
	}
	else
		return false;
}

void BigTable::print_bigtable(){
	unordered_map<string, unordered_map<string, File_data>> :: iterator iter;
	for(iter = bigtable.begin(); iter != bigtable.end(); iter++){
		unordered_map<string, File_data> :: iterator it;
		cout << iter->first << ": \n";
		for(it = bigtable[iter->first].begin(); it != bigtable[iter->first].end(); it++){
			cout << "--key: " << it->first << "  value: " << (it->second).username << ", " << (it->second).filename << "\n";
		}

	}
	cout << std::flush;
}


bool BigTable::flush_tables(string memtable_file, string bigtable_file, string deleted_file_name) {
	if (flush_memtable(memtable_file) && flush_bigtable(bigtable_file) && flush_deleted_files(deleted_file_name)) {
		return true;
	}
	return false;
}

bool BigTable::flush_memtable(string memtable_file) {
	string file_path_name = server_id + "/" + memtable_file;

	mem_file_mutex.lock();
	ofstream myfile(file_path_name);
	if (!myfile.is_open()) {
		cerr << "Cannot open the file: " << file_path_name << endl;
		mem_file_mutex.unlock();
		return false;
	}
	// write all the content of the memtable into the specified file.
	myfile.write((char *)&memtable[0], curr_pos);
	myfile.close();
	mem_file_mutex.unlock();
	return true;
}

bool BigTable::flush_bigtable(string bigtable_file) {
	string file_path_name = server_id + "/" + bigtable_file;

	bigtable_file_mutex.lock();
	ofstream myfile(file_path_name);
	if (!myfile.is_open()) {
		cerr << "Cannot open the file: " << file_path_name << endl;
		bigtable_file_mutex.unlock();
		return false;
	}

	// scan through the bigtable
	for (auto it = bigtable.begin(); it != bigtable.end(); it++) {
		for (auto it_inside = it->second.begin(); it_inside != it->second.end(); it_inside++) {
			myfile << it_inside->second.username << ";";
			myfile << it_inside->second.filename << ";";
			myfile << it_inside->second.file_type << ";";
			myfile << to_string(it_inside->second.buff_start) << ";";
			myfile << to_string(it_inside->second.file_length) << ";";
			myfile << to_string(it_inside->second.is_flushed) << ";";
			myfile << to_string(it_inside->second.is_deleted) << ";";
			myfile << it_inside->second.flush_filename;
			myfile << "\n";
		}
	}
	myfile.close();
	bigtable_file_mutex.unlock();
	return true;
}

bool BigTable::flush_deleted_files(string deleted_file_name) {
	string file_path_name = server_id + "/" + deleted_file_name;

	deleted_file_mutex.lock();
	ofstream myfile(file_path_name);
	if (!myfile.is_open()) {
		cerr << "Cannot open the file: " << file_path_name << endl;
		deleted_file_mutex.unlock();
		return false;
	}

	// scan through the deleted_files map
	for (auto it = deleted_files.begin(); it != deleted_files.end(); it ++) {
		string file_index_name = it->first;

		for (int i = 0; i < it->second.size(); i++) {
			myfile << file_index_name << ";";
			myfile << it->second[i].first << ";";
			myfile << it->second[i].second.username << ";";
			myfile << it->second[i].second.filename << ";";
			myfile << it->second[i].second.file_type << ";";
			myfile << to_string(it->second[i].second.buff_start) << ";";
			myfile << to_string(it->second[i].second.file_length) << ";";
			myfile << to_string(it->second[i].second.is_flushed) << ";";
			myfile << to_string(it->second[i].second.is_deleted) << ";";
			myfile << it->second[i].second.flush_filename;
			myfile << "\n";
		}
	}
	myfile.close();
	deleted_file_mutex.unlock();
	return true;
}




bool BigTable::load_memtable(string memtable_file) {
	// lock the file.
	mem_file_mutex.lock();

	curr_pos = 0;
	int fd;
	struct stat sb;
	off_t pa_offset;
	fd = open(memtable_file.c_str(), O_RDONLY);
	if (fd == -1) {
		cerr << "Fail to open" << endl;
		mem_file_mutex.unlock();
		return false;
	}

	// to obtain file size
	if (fstat(fd, &sb) == -1) {
		cerr << "Fail to get the status" << endl;
		mem_file_mutex.unlock();
		return false;
	}

    if (sb.st_size == 0) {
        cout << "recovery memtable is empty\n";
        mem_file_mutex.unlock();
        return true;
    }

	int offset = 0;
	pa_offset = offset & ~(sysconf(_SC_PAGE_SIZE) - 1); /* offset for mmap() must be page aligned */
	// if (offset >= sb.st_size) {
	// 	cerr << "offset is past end of file" << endl;
	// 	mem_file_mutex.unlock();
	// 	return false;
	// }



	char * addr;
	addr = (char *) mmap(NULL, sb.st_size - pa_offset, PROT_READ, MAP_PRIVATE, fd, pa_offset);
	if (addr == MAP_FAILED) {
		cerr << "mmap fail" << endl;
		mem_file_mutex.unlock();
		return false;
	}

	for (int i = 0; i < sb.st_size; i++) {
		memtable[i] = *(addr + offset - pa_offset + i);
		curr_pos++;
	}
	
	// unmap and close the fd
	munmap(addr, sb.st_size - pa_offset);
	close(fd);
	mem_file_mutex.unlock();
    return true;
}

bool BigTable::load_bigtable(string bigtable_file) {
	// also need to update the memory_files vector by checking whether it is flushed.
	string line;

	bigtable_file_mutex.lock();
	ifstream myfile(bigtable_file);
	file_index = -1;
	if (!myfile.is_open()) {
		cerr << "Cannot open the file: " << bigtable_file << endl;
		file_index = 0;
		bigtable_file_mutex.unlock();
		return false;
	}
	bool in_memory = false;

	while (getline(myfile, line)) {
		vector<string> fields;
		while (line.find(";") != string::npos) {
			size_t pos = line.find(";");
			fields.push_back(line.substr(0, pos));
			line = line.substr(pos + 1);
		}
		fields.push_back(line);

		if (fields.size() != 8) continue;
		string username = fields[0];
		string filename = fields[1];
		string file_type = fields[2];
		int buff_start = atoi(fields[3].c_str());
		int file_length = atoi(fields[4].c_str());
		bool is_flushed = atoi(fields[5].c_str());
		bool is_deleted = atoi(fields[6].c_str());
		string flush_filename = fields[7];
		file_index = max(file_index, atoi(flush_filename.c_str()));

		// adding to the memory files if this is not flushed.
		if (!is_flushed) {
			memory_files.push_back(username + "/" + filename);
			in_memory = true;
		} else {
			if (disk_files.find(flush_filename) == disk_files.end()) {
				disk_files.emplace(piecewise_construct, forward_as_tuple(flush_filename), forward_as_tuple());
			}
			disk_files.at(flush_filename).push_back(username + "/" + filename);

			if (file_mutex.find(flush_filename) == file_mutex.end()) {
				file_mutex.emplace(piecewise_construct, forward_as_tuple(flush_filename), forward_as_tuple());
			}
		}

		if (bigtable.find(username) == bigtable.end()) {
			bigtable.emplace(piecewise_construct, forward_as_tuple(username), forward_as_tuple());
		}
		File_data temp(username, filename, file_type, buff_start, file_length, is_flushed, is_deleted, flush_filename);
		bigtable.at(username).emplace(filename, temp);
	}
	myfile.close();
	// if the memtable is empty, the we know that it is in the next flush_file.
	if (!in_memory) file_index++;
	for (int i = 0; i <= file_index; i++) {
		deleted_files.emplace(piecewise_construct, forward_as_tuple(to_string(i)), forward_as_tuple());
	}

	bigtable_file_mutex.unlock();
}

bool BigTable::load_deleted_files(string deleted_file_name) {
	string line;
	deleted_file_mutex.lock();

	ifstream myfile(deleted_file_name);
	if (!myfile.is_open()) {
		cerr << "Cannot open the file: " << deleted_file_name << endl;
		deleted_files.emplace(piecewise_construct, forward_as_tuple(to_string(file_index)), forward_as_tuple());
		deleted_file_mutex.unlock();
		return false;
	}
	bool in_memory = false;
	while (getline(myfile, line)) {
		vector<string> fields;
		while (line.find(";") != string::npos) {
			size_t pos = line.find(";");
			fields.push_back(line.substr(0, pos));
			line = line.substr(pos + 1);
		}
		fields.push_back(line);

		if (fields.size() != 10) continue;
		string file_index_name = fields[0];
		time_t time = atoi(fields[1].c_str());
		string username = fields[2];
		string filename = fields[3];
		string file_type = fields[4];
		int buff_start =  atoi(fields[5].c_str());
		int file_length = atoi(fields[6].c_str());
		bool is_flushed = atoi(fields[7].c_str());
		bool is_deleted = atoi(fields[8].c_str());
		string flush_filename = fields[9];

		if (!is_flushed) in_memory = true;

		if (deleted_files.find(file_index_name) == deleted_files.end()) {
			deleted_files.emplace(piecewise_construct, forward_as_tuple(file_index_name), forward_as_tuple());
		}
		File_data temp(username, filename, file_type, buff_start, file_length, is_flushed, is_deleted, flush_filename);
		deleted_files.at(file_index_name).push_back(make_pair(time, temp));
	}
	myfile.close();
	if (!in_memory) {
		deleted_files.emplace(piecewise_construct, forward_as_tuple(to_string(file_index)), forward_as_tuple());
	}
	deleted_file_mutex.unlock();
}


bool BigTable::clear_directory(string s) {
	boost::filesystem::path dir(s);
	if (boost::filesystem::exists(dir)) {
		boost::filesystem::remove_all(dir);
	}
}

bool BigTable::rename_file(string username, string oldfilename, string newfilename) {
    // modify the corresponding filename in the bigtable
    cout << "rename_file\n";
    if (bigtable.find(username) == bigtable.end() || bigtable[username].find(oldfilename) == bigtable[username].end()) {
        return false;
    } else {
        cout << "rename_file1\n";
        if (bigtable.at(username).at(oldfilename).is_deleted) {
            return false;
        }
        cout << "rename_file2\n";
        if (bigtable[username].find(newfilename) != bigtable[username].end()) {
            return false;
        }
        cout << "rename_file3\n";
        bool in_memory = !(bigtable.at(username).at(oldfilename).is_flushed);
        string flushfile_temp = bigtable.at(username).at(oldfilename).flush_filename;
        bigtable.at(username).emplace(newfilename, bigtable.at(username).at(oldfilename));
        bigtable.at(username).erase(oldfilename);
        bigtable.at(username).at(newfilename).filename = newfilename;

        cout << "rename_file4\n";
        if (in_memory) { 
            cout << "rename_file5\n";   
            int pos = 0;
            for (; pos < memory_files.size(); pos++) {
                if (memory_files[pos] == (username + "/" + oldfilename)) {
                    break;
                }
            }
            memory_files.erase(memory_files.begin() + pos);
            memory_files.push_back(username + "/" + newfilename);
        } else {
            cout << "rename_file6\n";   
            int pos = 0;
            for (; pos < disk_files.at(flushfile_temp).size(); pos++) {
                if (disk_files.at(flushfile_temp)[pos] == (username + "/" + oldfilename)) {
                    break;
                }
            }
            disk_files.at(flushfile_temp).erase(disk_files.at(flushfile_temp).begin() + pos);
            disk_files.at(flushfile_temp).push_back(username + "/" + newfilename);
        }
        cout << "rename_file7\n";   
        return true;
    }
}






