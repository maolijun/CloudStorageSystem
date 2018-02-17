#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <iostream>
#include <pthread.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iomanip>
using namespace std;

#define MAXLEN 40960000
#define ERRCODE -1

/*master node address and port(command line arg)*/
char master_ip[] = "127.0.0.1";
int master_port;

/*smtp server address and port(command line arg)*/
char smtp_ip[] = "127.0.0.1";
int smtp_port;

/*load balancer address and port(command line arg)*/
char balancer_ip[] = "127.0.0.1";
int balancer_port;

//string test_file = "";
string local_address; //the address of the server itself

/*user's files and directories information stored in memory*/
unordered_map<string, string> user_current_dir; //user's current relative directory path
unordered_map<string, unordered_map<string, string> > user_files; //user's files and directory after calling VIEW
unordered_map<string, unordered_map<string, string> > user_emails; // user's emails when calling VIEW

/*client information*/
struct client{
	int fd;
	int port;
	char* ip;
};

/*used to read the large file from web client, use timeout as 0.1s*/
int echo_read(int fd, char* request, int size){
	int length = 0;
	while(true){
		fd_set set;
		struct timeval timeout;
		FD_ZERO(&set);
		FD_SET(fd, &set);
		timeout.tv_sec = 0;
		timeout.tv_usec = 100000;
		int ret = select(fd + 1, &set, NULL, NULL, &timeout);//stop reading when timing out
		if(ret == 1){
			int n = read(fd, &request[length], size-length);
			length += n;
			request[length] = '\0';
		} else {
			break;
		}
	}
	//fprintf(stderr, "%s\n", request);
	return length;
}

/*read the content of specified length*/
int do_read(int fd, char* buf, int len){
	int rcvd = 0;  
	while (rcvd < len) {
		int n = read(fd, &buf[rcvd], len-rcvd);
		if (n<0){
			return -1;
		}
		rcvd += n;
	}
	return len;
}

/*write the content of the specified response*/
int do_write(int fd, const char* buf, int len){
	int sent = 0;  
	while (sent < len) {
		int n = write(fd, &buf[sent], len-sent);
		if (n<0){
			return -1;
		}
		sent += n;
	}
	return len;
}

/*get the address the server itself by reading the server address of from the http config file with line index*/
string get_local_address(int index, string config){
	ifstream infile;
	infile.open(config);
	if(!infile.is_open()){
		fprintf(stderr, "The file could not be opened.\n");
		infile.close();
		exit(1);
	}
	string line;
	int i = 1;
	while(getline(infile, line)){
		if(i == index){
			infile.close();
			return line;
		}
		i++;
	}
	cout << "Http Server index out of range. Server index range from 1 to " << i-1 << "." << endl; // index out of range
	infile.close();
	exit(1);
}

/*get the adresses of the storage node(primary and replica)*/
vector<string> get_storage_address(string addresses){
	vector<string> ret;
	string check_string = addresses;
	while(check_string.find("#") != -1){
		string temp = check_string.substr(0, check_string.find("#"));
		ret.push_back(temp);
		check_string = check_string.substr(check_string.find("#")+1);
	}
	ret.push_back(check_string);
	return ret;
}

/*transfer the %40 to @ label*/
string transfer_at_label(string email_address){
	size_t at_label = email_address.find("%40");
	return email_address.substr(0, at_label) + "@" + email_address.substr(at_label+3);
}

/*connect to the storage node with the given address and request*/
string get_storage_response(string address, string request){
	size_t split = address.find(":");
	string ip = address.substr(0, split);
	int port = stoi(address.substr(split+1));
	int storage_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(storage_fd < 0){
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		return "-ERR";
	}
	struct sockaddr_in storageaddr;
	bzero(&storageaddr, sizeof(storageaddr));
	storageaddr.sin_family = AF_INET;
	storageaddr.sin_port = htons(port);
	inet_pton(AF_INET, ip.c_str(), &(storageaddr.sin_addr));
	int can_connect = connect(storage_fd, (struct sockaddr*)&storageaddr, sizeof(storageaddr));
	if(can_connect < 0){
		fprintf(stderr, "Can not connect (%s)\n", strerror(errno));
	}
	//write the request to the storage node
	string write_len = to_string(strlen(request.c_str()));//write request length
	write(storage_fd, write_len.c_str(), strlen(write_len.c_str()));
	char *storage_res = new char[MAXLEN];
	recv(storage_fd, storage_res, MAXLEN, 0);
	do_write(storage_fd, request.c_str(), strlen(request.c_str()));//write request

	char read_digit[10];
	int d = read(storage_fd, read_digit, 10);
	read_digit[d] = '\0';
	cout << "res length: " << read_digit << endl;
	string ack = "+OK";
	send(storage_fd, ack.c_str(), ack.length(), 0);//send ack

	int bytes = do_read(storage_fd, storage_res, atoi(read_digit));
	storage_res[bytes] = '\0';
	cout << "res: " << storage_res << endl;
	string res(storage_res);
	delete[] storage_res;
	close(storage_fd);
	return res;
}

/*notify the load balancer if the server is started or shut down*/
string notify_balancer(bool is_start){
	int balancer_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(balancer_fd < 0){
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		return "";
	}
	struct sockaddr_in balancer_addr;
	bzero(&balancer_addr, sizeof(balancer_addr));
	balancer_addr.sin_family = AF_INET;
	balancer_addr.sin_port = htons(balancer_port);
	inet_pton(AF_INET, balancer_ip, &(balancer_addr.sin_addr));
	int can_connect = connect(balancer_fd, (struct sockaddr*)&balancer_addr, sizeof(balancer_addr));
	if(can_connect < 0){
		fprintf(stderr, "Can not connect (%s)\n", strerror(errno));
	}
	if(is_start){
		string start_message = "POST /start " + local_address;
		write(balancer_fd, start_message.c_str(), strlen(start_message.c_str()));
	} else {
		string shut_message = "POST /shutdown " + local_address;
		write(balancer_fd, shut_message.c_str(), strlen(shut_message.c_str()));
	}
	close(balancer_fd);
	return "+OK";
}

/*send request to smtp server if write to remote webmail*/
string smtp_send_email(string address, string request){
	size_t split = address.find(":");
	string ip = address.substr(0, split);
	int port = stoi(address.substr(split+1));
	int smtp_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(smtp_fd < 0){
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		return "-ERR";
	}
	struct sockaddr_in smtp_addr;
	bzero(&smtp_addr, sizeof(smtp_addr));
	smtp_addr.sin_family = AF_INET;
	smtp_addr.sin_port = htons(port);
	inet_pton(AF_INET, ip.c_str(), &(smtp_addr.sin_addr));
	connect(smtp_fd, (struct sockaddr*)&smtp_addr, sizeof(smtp_addr));
	//write the request to the smtp node
	write(smtp_fd, request.c_str(), strlen(request.c_str()));
	char smtp_res[8192];
	int bytes = read(smtp_fd, smtp_res, 8192);
	smtp_res[bytes] = '\0';
	string res(smtp_res);
	close(smtp_fd);
	return res;	
}

/*parse the file and directory from the message string returned by master node*/
vector<string> depack(string msg, char breaking_siign){
	vector<string> results;
	int index = 0;
	int last_index = 0;
	for (; index < msg.size(); index++){
		if (msg.at(index) == breaking_siign){
			results.push_back(msg.substr(last_index, index - last_index));
			last_index = index + 1;
		}
	}
	results.push_back(msg.substr(last_index));
	return results;
}

/*get each file/directory/email from the message and return as a map*/
unordered_map<string, string> get_file_view(string msg){
	unordered_map<string, string> filepath_filetype_mapping;
	vector<string> msg_depack = depack(msg, '#');
	for (int i = 0; i < msg_depack.size()-1; i += 2){
		filepath_filetype_mapping.insert(make_pair(msg_depack[i], msg_depack[i+1]));
	}
	return filepath_filetype_mapping;
}

/*detect if the file name start with the prefix*/
bool starts_with(string content, string prefix){
	return content.substr(0, prefix.size()) == prefix;
}

/*get the files, folders or emails in the current directory*/
vector<string> getFileType(unordered_map<string, string> map, string path, string type){
	unordered_map<string, string>::iterator it = map.begin();
	vector<string> ret_value;
	while(it != map.end()){
		string prefix = it->first.substr(0, path.length()); // compare each file/folder prefix path with current folder
		if(prefix == path){
			string rest = it->first.substr(path.length());
			if(rest.find("/") == -1 && it->second == type){ // check if the type is corrent and the file/folder is in current folder
				ret_value.push_back(rest);
			}
		}
		it++;
	}
	return ret_value;
}

/*given the current directory, go back to the last directory*/
string goback_dir(string current){
	string temp = current.substr(0, current.length()-1);
	if(temp.find("/") == -1) return "";
	return temp.substr(0, temp.find_last_of("/")+1);
}

/*delete the selected folder as well as all the files/folders under the folder*/
void delete_folder(string folder, string cookie, int master_fd){
	unordered_map<string, string> files_to_delete;
	unordered_map<string, string>::iterator it = user_files[cookie].begin();
	//record all the files/folder under the current folder
	while(it != user_files[cookie].end()){
		string prefix = it->first.substr(0, folder.length());
		if(prefix == folder){
			files_to_delete[it->first] = it->second;
		} 
		it++;
	}
	it = files_to_delete.begin();
	//delete files/folders from memory
	while(it != files_to_delete.end()){
		string master_delete = "DEL " + cookie + "#" + it->first + "#" + it->second;
		write(master_fd, master_delete.c_str(), strlen(master_delete.c_str()));
		char delete_res[200];
		int bytes = read(master_fd, delete_res, 200);
		delete_res[bytes] = '\0';
		string response(delete_res);
		string can_delete = response.substr(0, response.find(" "));
		vector<string> storage_addr = get_storage_address(response.substr(response.find(" ")+1));
		if(can_delete == "+OK"){
			string storage_res = "-ERR";
			for(int i = 0; i < storage_addr.size(); i++){
				string temp = get_storage_response(storage_addr[i], master_delete);
				if(temp == "+OK") storage_res = temp;
			}
			if(storage_res == "+OK"){
				user_files[cookie].erase(it->first);
			}
			//user_files[cookie].erase(it->first);
		}
		it++;
	}
}

/*delete the selected file*/
void delete_file(string file, string cookie, int master_fd){
	string master_delete = "DEL " + cookie + "#" + file + "#" + "DRIVE";
	write(master_fd, master_delete.c_str(), strlen(master_delete.c_str()));
	char delete_res[200];
	int bytes = read(master_fd, delete_res, 200);
	delete_res[bytes] = '\0';
	string response(delete_res);
	string can_delete = response.substr(0, response.find(" "));
	vector<string> storage_addr = get_storage_address(response.substr(response.find(" ")+1));
	if(can_delete == "+OK"){
		string storage_res = "-ERR";
		for(int i = 0; i < storage_addr.size(); i++){
			string temp = get_storage_response(storage_addr[i], master_delete);
			if(temp == "+OK") storage_res = temp;
		}
		if(storage_res == "+OK"){
			user_files[cookie].erase(file);
		}
		//user_files[cookie].erase(file);
	}
}

void delete_email(string email, string cookie, int master_fd){
	string master_delete = "DEL " + cookie + "#" + email + "#" + "EMAIL";
	write(master_fd, master_delete.c_str(), strlen(master_delete.c_str()));
	char delete_res[200];
	int bytes = read(master_fd, delete_res, 200);
	delete_res[bytes] = '\0';
	string response(delete_res);
	string can_delete = response.substr(0, response.find(" "));
	vector<string> storage_addr = get_storage_address(response.substr(response.find(" ")+1));
	if(can_delete == "+OK"){
		string storage_res = "-ERR";
		for(int i = 0; i < storage_addr.size(); i++){
			string temp = get_storage_response(storage_addr[i], master_delete);
			if(temp == "+OK") storage_res = temp; 			
		}
		if(storage_res == "+OK"){
			user_emails[cookie].erase(email);
		}
		//user_emails[cookie].erase(email);
	}
}

/*rename the selected file*/
void rename_file(string original, string modified, string cookie, int master_fd){
	string curr = user_current_dir[cookie];
	string master_req = "MOVE " + cookie + "#" + curr+original + "#" + curr+modified + "#DRIVE";
	write(master_fd, master_req.c_str(), strlen(master_req.c_str()));
	char master_res[200];
	int bytes = read(master_fd, master_res, 200);
	master_res[bytes] = '\0';
	string res(master_res);
	string status = res.substr(0, res.find(" "));
	vector<string> storage_addr = get_storage_address(res.substr(res.find(" ")+1));
	if(status == "+OK"){
		string storage_res = "-ERR";
		for(int i = 0; i < storage_addr.size(); i++){
			string temp = get_storage_response(storage_addr[i], master_req);
			if(temp == "+OK") storage_res = temp;
		}
		if(storage_res == "+OK"){
			user_files[cookie].erase(curr+original);
			user_files[cookie][curr+modified] = "DRIVE";
		}
		//user_files[cookie].erase(curr+original);
		//user_files[cookie][curr+modified] = "DRIVE";
	}
}

/*rename the folder and the files/folders under it*/
void rename_folder(string original, string modified, string cookie, int master_fd){
	string curr = user_current_dir[cookie];
	string f = curr+original;
	unordered_map<string, string> files_to_rename;
	unordered_map<string, string>::iterator it = user_files[cookie].begin();
	//record the files and folder's under the selected folder
	while(it != user_files[cookie].end()){
		string prefix = it->first.substr(0, f.length());
		if(f == prefix){
			files_to_rename[it->first] = it->second;
		}
		it++;
	}
	it = files_to_rename.begin();
	//rename each folder and file address with the modified folder name
	while(it != files_to_rename.end()){
		string updated = curr + modified + it->first.substr(f.length());
		string master_req = "MOVE " + cookie + "#" + it->first + "#" + updated + "#" + it->second;
		write(master_fd, master_req.c_str(), strlen(master_req.c_str()));
		char master_res[200];
		int bytes = read(master_fd, master_res, 200);
		master_res[bytes] = '\0';
		string response(master_res);
		string can_delete = response.substr(0, response.find(" "));
		vector<string> storage_addr = get_storage_address(response.substr(response.find(" ")+1));
		if(can_delete == "+OK"){
			string storage_res = "-ERR";
			for(int i = 0; i < storage_addr.size(); i++){
				string temp = get_storage_response(storage_addr[i], master_req);
				if(temp == "+OK") storage_res = temp;				
			}
			if(storage_res == "+OK"){
				user_files[cookie].erase(it->first);
				user_files[cookie][updated] = it->second;
			}
			//user_files[cookie].erase(it->first);
			//user_files[cookie][updated] = it->second;
		}
		it++;		
	}
}

/*move the selected file to the specified folder*/
void move_file(string file, string folder, string cookie, int master_fd){
	string file_addr = user_current_dir[cookie] + file;
	string target_folder = "";
	if(folder != "RootFolder"){
		unordered_map<string, string>::iterator it = user_files[cookie].begin();
		while(it != user_files[cookie].end()){
			if(it->second == "FOLDER"){
				string f = it->first;
				if(f.find("/") == -1){
					if(f == folder){
						target_folder = f + "/";
						break;
					}
				} else {
					string temp = f.substr(f.find_last_of("/")+1);
					if(temp == folder){
						target_folder = f + "/";
						break;
					}
				}

			}
			it++;
		}
	}
	string master_req = "MOVE " + cookie + "#" + file_addr+ "#" + target_folder + file + "#DRIVE";
	write(master_fd, master_req.c_str(), strlen(master_req.c_str()));
	char master_res[200];
	int bytes = read(master_fd, master_res, 200);
	master_res[bytes] = '\0';
	string res(master_res);
	string status = res.substr(0, res.find(" "));
	vector<string> storage_addr = get_storage_address(res.substr(res.find(" ")+1));
	if(status == "+OK"){
		string storage_res = "-ERR";
		for(int i = 0; i < storage_addr.size(); i++){
			string temp = get_storage_response(storage_addr[i], master_req);
			if(temp == "+OK") storage_res = temp;
		}
		if(storage_res == "+OK"){
			user_files[cookie].erase(file_addr);
			user_files[cookie][target_folder+file] = "DRIVE";
		}
		//user_files[cookie].erase(file_addr);
		//user_files[cookie][target_folder+file] = "DRIVE";
	}
}

/*used for debug(print out all user's file in the terminal)*/
void print_files(unordered_map<string, string> map){
	unordered_map<string, string>::iterator it = map.begin();
	while(it != map.end()){
		fprintf(stderr, "%s   %s\n", it->first.c_str(), it->second.c_str());
		it++;
	}
	cout << "\n\n"<< endl;
}

/*render a static page with client fd, html page name, additional information and optional cookie */
void render_page_with_info(int fd, string page, string info, string cookie){

	page = "html/" + page;
	ifstream file(page.c_str());
    string line;
    string message_body;
    string status = "HTTP/1.1 200 OK\r\n";
	string length = "Content-Length: ";
	string type = "Content-Type: text/html\r\n";
	if(cookie != ""){
		cookie = "Set-Cookie: username=" + cookie + "\r\n"; //set cookie for the response
	}
    while (getline(file, line))
    {
    	if(line == "</body>"){
    		message_body += "<br></br>\n";
    		message_body += ("<center><b>" + info + "</b></center>\n"); //set additional information for the page
    	}
    	message_body += line;
    }
    file.close();
   	string html = status + length + to_string(message_body.length()) + "\r\n" + type + cookie + "\r\n" + message_body;
	write(fd, html.c_str(), strlen(html.c_str()));
}

string add_mark(string message){
	string ret;
	while(message.find("\r\n") != -1){
		ret += ">>";
		ret += message.substr(0, message.find("\r\n")+2);
		message = message.substr(message.find("\r\n")+2);
	}
	ret += ">>";
	ret += message;
	return ret;
}

void open_email(int fd, string message, string email_name){
	string message_body;
	string status = "HTTP/1.1 200 OK\r\n";
	string length = "Content-Length: ";
	string type = "Content-Type: text/html\r\n";
	message_body += "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\" />\n<title>PennCloud-Email</title>\n</head>\n<body>";
	message_body += "Title: " + email_name.substr(0, email_name.find("-")) + "<br><br>";
	message_body += "Sender: " + email_name.substr(email_name.find("-")+1) + "@penncloud.com<br><br>";
	message_body += "Content:<br>";
	message_body += "<textarea rows=\"10\" col=\"100\" readonly=\"readonly\">" + message + "</textarea>";
	message_body += "<br><br><br><a href=/cancel>Go Back</a></body>";
	string html = status + length + to_string(message_body.length()) + "\r\n" + type + "\r\n" + message_body;
	write(fd, html.c_str(), strlen(html.c_str()));
}

void forward_email(int fd, string message, string email_name, string sender){
	string message_body;
	string status = "HTTP/1.1 200 OK\r\n";
	string length = "Content-Length: ";
	string type = "Content-Type: text/html\r\n";
	message_body += "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\" />\n<title>PennCloud-Email</title>\n</head>\n<body>";
	message_body += "<form action=\"/forward\" method=\"post\" enctype=\"text/plain\">";
	message_body += "To: ";
	message_body += "<br><input type=\"text\" name=\"to\"";
	message_body += "><br><br>";

	message_body += "Title: ";
	message_body += "<br><input type=\"text\" name=\"title\" value=\"Fwd:";
	message_body += email_name.substr(0, email_name.find("-"));
	message_body += "\" readonly=\"readonly\"><br><br>";

	message_body += "Content:<br><textarea rows=\"10\" col=\"100\" name=\"content\">" + add_mark(message) + "</textarea><br><br>";
	message_body += "<button type=\"submit\" value=\"Send\">Forward</button></form>";
	message_body += "<br><br><br><a href=/cancel>Go Back</a></body>";
	string html = status + length + to_string(message_body.length()) + "\r\n" + type + "\r\n" + message_body;
	write(fd, html.c_str(), strlen(html.c_str()));
}

void reply_email(int fd, string message, string email_name){
	string message_body;
	string status = "HTTP/1.1 200 OK\r\n";
	string length = "Content-Length: ";
	string type = "Content-Type: text/html\r\n";
	message_body += "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\" />\n<title>PennCloud-Email</title>\n</head>\n<body>";
	message_body += "<form action=\"/reply\" method=\"post\" enctype=\"text/plain\">";
	message_body += "To: ";
	message_body += "<br><input type=\"text\" name=\"to\" value=\"";
	message_body += email_name.substr(email_name.find("-")+1);
	message_body += "@penncloud.com";
	message_body += "\" readonly=\"readonly\"><br><br>";

	message_body += "Title: ";
	message_body += "<br><input type=\"text\" name=\"title\" value=\"Re:";
	message_body += email_name.substr(0, email_name.find("-"));
	message_body += "\" readonly=\"readonly\"><br><br>";

	message_body += "Content:<br><textarea rows=\"10\" col=\"100\" name=\"content\">" + add_mark(message) + "</textarea><br><br>";
	message_body += "<button type=\"submit\" value=\"Send\">Reply</button></form>";
	message_body += "<br><br><br><a href=/cancel>Go Back</a></body>";
	string html = status + length + to_string(message_body.length()) + "\r\n" + type + "\r\n" + message_body;
	write(fd, html.c_str(), strlen(html.c_str()));
}


/*get all the folders of the current user*/
vector<string> getAllFolders(string cookie){
	vector<string> folders;
	unordered_map<string, string>::iterator it = user_files[cookie].begin();
	while(it != user_files[cookie].end()){
		if(it->second == "FOLDER"){
			string f = it->first;
			if(f.find("/") == -1){
				folders.push_back(f);
			} else {
				folders.push_back(f.substr(f.find_last_of("/")+1));
			}
		}
		it++;
	}
	return folders;
}

/*get all the emails of the login user from the emails map*/
vector<string> getEmails(unordered_map<string, string> map){
	unordered_map<string, string>::iterator it = map.begin();
	vector<string> ret_value;
	while(it != map.end()){
		ret_value.push_back(it->first);
		it++;
	}
	return ret_value;
}

/*make the option field for the move command in the file/folder list page*/
string getpathOptions(string cookie){
	string options = "";
	vector<string> folders = getAllFolders(cookie);
	options += "<option value=\"RootFolder\">RootFolder</option>\n";
	for(int i = 0; i < folders.size(); i++){
		options += "<option value=\"";
		options += folders[i] + "\">";
		options += folders[i];
		options += "</option>\n";
	}
	return options;
}

/*render dynamic html pages listing all emails fo the user*/
void render_dynamic_email_page(int fd, string username){
	string message_body;
    string status = "HTTP/1.1 200 OK\r\n";
	string length = "Content-Length: ";
	string type = "Content-Type: text/html\r\n";
	message_body += "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\" />\n<title>PennCloud-Email</title>\n</head>\n";
	message_body += "<body>\n<center>\n<h1>Inbox Email for user: " + username + "</h1>\n</center>";
	vector<string> emails = getEmails(user_emails[username]);

	message_body += "<b>Emails:</b><br><table cellspacing=\"12\"><tr><th>Title</th><th>Sender</th><th>Delete</th><th>Forward</th><th>Reply</th></tr>";
    for(int i = 0; i < emails.size(); i++){
    	//open the selected folder
    	message_body += "<tr><td><form action=\"/reademail\" method=\"post\" enctype=\"text/plain\"><button name=\"";
    	message_body += emails[i];
    	message_body += "\">";
    	message_body += emails[i].substr(0, emails[i].find("-"));
    	message_body += "</button></form></td>";

    	//show the sender of the email
    	message_body += "<td>";
    	message_body += (emails[i].substr(emails[i].find("-")+1)+"@penncloud.com");
    	message_body += "</td>";
    	
    	//delete the selected folder
    	message_body += "<td><form action=\"/deleteemail\" method=\"post\" enctype=\"text/plain\"><button name=\"";
    	message_body += emails[i];
    	message_body += "\">delete";
    	message_body += "</button></form></td>";

    	//forward the selected folder
    	message_body += "<td><form action=\"/forwardemail\" method=\"post\" enctype=\"text/plain\"><button name=\"";
    	message_body += emails[i];
    	message_body += "\">forward";
    	message_body += "</button></form></td>";

    	//reply the selected folder
    	message_body += "<td><form action=\"/replyemail\" method=\"post\" enctype=\"text/plain\"><button name=\"";
    	message_body += emails[i];
    	message_body += "\">reply";
    	message_body += "</button></form></td></tr>";
    }
    message_body += "</table><br>";

    // create a new email
    message_body += "<br><b>Write a new email:</b><br>";
    message_body += "<a href=\"email.html\">Compose</a><br><br>";

    //link to other pages
    message_body += "<br></br>\n<a href=\"home.html\">HomePage</a>\n";
    message_body += "</center></body>\n</html>\n";
   	string html = status + length + to_string(message_body.length()) + "\r\n" + type + "\r\n" + message_body;
	write(fd, html.c_str(), strlen(html.c_str()));
}


/*render dynamic html pages listing all files and directories in the current directory*/
void render_dynamic_drive_page(int fd, string username){
    string message_body;
    string status = "HTTP/1.1 200 OK\r\n";
	string length = "Content-Length: ";
	string type = "Content-Type: text/html\r\n";
	message_body += "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\" />\n<title>PennCloud</title>";
	
	message_body += "<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css\" integrity=\"sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u\" crossorigin=\"anonymous\">";
	message_body += "</head><body>\n<center>\n<h1>Drive Dashboard for " + username + "</h1>\n</center>";
	unordered_map<string, string> all_files = user_files[username];
	string path = user_current_dir[username];
	vector<string> dir = getFileType(all_files, path, "FOLDER");
	vector<string> files = getFileType(all_files, path, "DRIVE");
	string options = getpathOptions(username);

	//message_body += "<center><b>Directory List</b>\n";
	message_body += "<div class=\"container\"><h3>Folders:</h3></div>";
	message_body += "<div class=\"container\"><table class=\"table\"><thead><tr><th>Name</th><th>Rename</th><th>Delete</th></tr></thead><tbody>";
	// message_body += "<b>Folders:</b><br><table cellspacing=\"12\"><tr><th>Name</th><th>Rename</th><th>Delete</th></tr>";
    for(int i = 0; i < dir.size(); i++){
    	//open the selected folder
    	message_body += "<tr><td><form action=\"/openfolder\" method=\"post\"><button name=\"";
    	message_body += dir[i];
    	message_body += "\">";
    	message_body += dir[i];
    	message_body += "</button></form></td>";

    	//rename the selected folder
    	message_body += "<td><form action=\"/renamefolder\" method=\"post\"><input type=\"text\" name=\"";
      	message_body += dir[i];
      	message_body += "\" placeholder=\"Enter new name\" required><input type=\"submit\" value=\"Rename\"></form></td>";
    	
    	//delete the selected folder
    	message_body += "<td><form action=\"/deletefolder\" method=\"post\"><button name=\"";
    	message_body += dir[i];
    	message_body += "\">delete";
    	message_body += "</button></form></td></tr>";
    }
    message_body += "</tbody></table>";

    // files
    message_body += "<div class=\"container\"><h3>Files:</h3></div>";
    message_body += "<table class=\"table\"><thead><tr><th>Download</th><th>Rename</th><th>Move to</th><th>Delete</th></tr></thead><tbody>";
    for(int i = 0; i < files.size(); i ++){
    	//downloads the selected file
      	message_body += "<tr><td><form><a href=\"/download/";
      	message_body += files[i] + "\" download>";
    	message_body += files[i] + "</a></form></td>";

      	//rename the selected file
      	message_body += "<td><form action=\"/renamefile\" method=\"post\"><input type=\"text\" name=\"";
      	message_body += files[i];
      	message_body += "\" placeholder=\"Enter new name\" required><input type=\"submit\" value=\"Rename\"></form></td>";
      	
      	// Move to another folder
      	message_body += "<td><form action=\"/movefile\" method=\"post\"><select name=\"";
      	message_body += files[i];
      	message_body += "\">";
      	message_body += options;
      	message_body += "</select><input type=\"submit\" value=\"Move\"></form></td>";
      	
      	// Delete the selected file
      	message_body += "<td><form action=\"/deletefile\" method=\"post\"><button name=\"";
      	message_body += files[i];
      	message_body += "\">delete</button></form></td>";
      	message_body += "</tr>";
    }
    message_body += "</tbody></table>";

    // create a new folder
    message_body += "<div class=\"container\"><h3>Add a new folder:</h3></div>";

    // message_body += "<br><table><tr><td><b>Add a new folder:</b></td><td></td></tr>";
    message_body += "<tr><td><form name=\"input\" action=\"/newfolder\" method=\"post\">";
    message_body += "<input type=\"text\" name=\"folder\" placeholder=\"New Folder\" size=\"20\" required></td>";
    message_body += "<td><input type=\"submit\" value=\"Submit\">";
    message_body += "</form></td></tr></table><br>";

    // upload a new file
	// message_body += "<tr><td><b>Add a new file:</b></td><td></td></tr>";
    message_body += "<div class=\"container\"><h3>Add a new file:</h3></div>";


   	message_body += "<tr><td><form name=\"input\" action=\"/newfile\" method=\"post\" enctype=\"multipart/form-data\">";
   	message_body += "<input type=\"file\" name=\"newfile\" required></td>";
   	message_body += "<td><input type=\"submit\" value=\"Upload\"></form></td></tr></table><br>";

    //link to other pages
    message_body += "<a href=\"goback\">Go Back</a>\n";
    message_body += "<br></br>\n<a href=\"home.html\">HomePage</a><br><br>\n";
    message_body += "</center></body>\n</html>\n";
   	string html = status + length + to_string(message_body.length()) + "\r\n" + type + "\r\n" + message_body;
	write(fd, html.c_str(), strlen(html.c_str()));
}

/*********************************************/
/*	      DRIVE: upload and download  		 */
/*********************************************/

void write_file(int fd, char* data, int content_len){

	write(fd, data, content_len);	
}


int char_to_int(char c) {
	int i;
	if(c >= '0' && c <= '9') i = c - '0';
	if(c >= 'A' && c <= 'F') i = c - 'A' + 10;
	if(c >= 'a' && c <= 'f') i = c - 'a' + 10;
	return i;
}

void decode_to_binary(char* bin_char, string hex_data) {
	const char * hex_char = hex_data.c_str();
	while(*hex_char && hex_char[1]) {
		*(bin_char++) = char_to_int(*hex_char)*16 + char_to_int(hex_char[1]);
		hex_char += 2;
	}
}

string extract_hex_string(string packet, int& content_len){
	size_t found;
	found = packet.find("----");
	string size = packet.substr(0, found);
	stringstream ss(size);
	string content = packet.substr(found + 4);
	ss >> content_len;
	return content;
}

// hex_packet: file_size----hexstring
void send_file(int fd, string hex_packet, string file_name){
	cout << "decoding to binary" << endl;
	int content_len;
	string hex_string = extract_hex_string(hex_packet, content_len);
	char* decoded_data = (char*) malloc(content_len + 1);
	decode_to_binary(decoded_data, hex_string);

	const static char* DOWNLOAD_HEADER = "HTTP/1.0 200 OK\nContent-Type: ";
	const static char* DOWNLOAD_HEADER2 = "\nContent-Disposition: attachment; filename=\"";
	const static char* DOWNLOAD_HEADER3 = "\"\nContent-Length: ";	
	string content_type = "multipart/form-data";
	string response = DOWNLOAD_HEADER;
	response += content_type;
	response += DOWNLOAD_HEADER2;
	response += file_name;
	response += DOWNLOAD_HEADER3;
	stringstream savedDataLen_ss;
	savedDataLen_ss << content_len;
	response += savedDataLen_ss.str() + "\n";
	response += "\n";
	cout << "writing file to browser" << endl;
	write(fd, response.c_str(), response.length());
	write_file(fd, decoded_data, content_len);
	free(decoded_data);
}

string encode_to_hex(int content_len, const char* bytes_data) {
	stringstream ss;
	// ss << std::hex << std::setw(2) << std::setfill('0');
	for (int i = 0; i < content_len; i++) {
		// convert to byte first using unsigned char
		unsigned char byte = (unsigned char) bytes_data[i];
		// convert to hex
		ss << hex << setw(2) << setfill('0') << (int)byte;
	}
	return ss.str();
}

char* trim_bytes_data(const char* ori_data, int& trim_len){
	int end = 0;
	// find end of content
	while (strncmp(ori_data + end, "----", 4)){
		end++;
	}
	trim_len = end - 2; // also remove \r\n
	char* parsed_data = (char*) malloc(sizeof(char) * (trim_len + 1));
	for (int i = 0; i < trim_len; i++){
		parsed_data[i] = ori_data[i];
	}
	return parsed_data;
}

char* get_bytes_data(const char* content_buf, int content_len) {
	int start = 0;
	// find the index pointing to the start of content after two "\r\n\r\n"
	while (strncmp(content_buf + start, "\r\n\r\n", 4)){
		start++;
	}
	start += 4;
	while (strncmp(content_buf + start, "\r\n\r\n", 4)){
		start++;
	}
	start += 4;
	// end pointer of content	
	int end = start + content_len;
	char* content = (char*) malloc(sizeof(char) * (end - start + 1));
//	strncpy(content, content_buf + start, end - start);
	const char* content_head = content_buf + start;
	for (int i = 0; i < end - start; i++){
		content[i] = *(content_head + i);
	}
	return content;
}


string insert_file_size(string hex_string, int content_len){
	stringstream ss;
	ss << content_len;
	// use "----" to saperate file length and hex string
	string result = ss.str() + "----" + hex_string;
	return result;
}

string generate_new_file(char* content_buf, int content_len) {

	cout << "generating raw data" << endl;
	char* ori_data = get_bytes_data(content_buf, content_len);

	int trim_len = 0;
	cout << "triming data" << endl;
	char* parsed_data = trim_bytes_data(ori_data, trim_len);
	free(ori_data);

	cout << "encoding data" << endl;
	string hex_data = encode_to_hex(trim_len, parsed_data);
	string hex_packet = insert_file_size(hex_data, trim_len);

	// cout << "test decode: \n";
	// int dep_len;
	// string dep = extract_hex_string(packet, dep_len);

	// cout << "decoding data" << endl;
	// char* decoded_data = (char*) malloc(dep_len + 1);
	// decode_to_binary(decoded_data, dep);
	free(parsed_data);
	return hex_packet;
}

vector<string> parse_upload(char* request){
	vector<string> file_info;
	string str(request);
	size_t found;
	string rest;
	string file_name;

	string filename_key = "filename=";
	string length_header = "Content-Length: ";
	
	// // parse file name
	found = str.find(filename_key);
	rest = str.substr(found);
	found = rest.find("\r\n");
 	file_name = rest.substr(filename_key.size()+1, found - (filename_key.size()+2));
 	// FILENAME = file_name;
	cout << "file_name: " << file_name << endl;
	
	int file_size;
	found = str.find(length_header);
	rest = str.substr(found);
	found = rest.find("\r\n");
	file_size = stoi(rest.substr(length_header.size(), found - length_header.size()));
	cout << "file_size: " << file_size << endl;	
	string file_hex = generate_new_file(request, file_size);

	file_info.push_back(file_name);
	file_info.push_back(file_hex);
	return file_info;
}

/*********************************************/
/*	      DRIVE: upload and download  		 */
/*********************************************/

/*the worker thread when a new connection is established*/
void *worker(void *arg){
	struct client *c = (struct client *)arg;
	int fd = c->fd;
	int client_port = c->port;
	char* client_ip = c->ip;

	/*receive http request from browser*/
	char* request = new char[MAXLEN];
	int read_bytes = echo_read(fd, request, MAXLEN);

	/*check connection error or termination*/
	if(read_bytes <= 0){
		if(read_bytes < 0){
			fprintf(stderr, "Error when reading from socket (%s)\n", strerror(errno));
		} else {
			fprintf(stderr, "Client  %s:%d fd:[%d] terminates\n", client_ip, client_port, fd);
		}
		free(c);
		delete[] request;
		close(fd);
		pthread_exit(NULL);

	}

	/*parse http-verb and command from request*/
	request[read_bytes] = '\0';
	string req(request);
	string http_verb = req.substr(0, req.find(" "));
	string rest = req.substr(req.find(" ")+1);
	string command = rest.substr(0, rest.find(" "));
	//fprintf(stderr, "%s\n", request);

	/*check the cookie*/
	string cookie = "NULL";
	size_t cookie_index = rest.find("username=");
	if(cookie_index != -1){
		string temp = rest.substr(cookie_index);
		cookie = temp.substr(9, temp.find("\r\n")-9); //get the username cookie information
	}

	/*create the socket with the master node*/
	int master_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(master_fd < 0){
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		close(fd);
		free(c);
		delete[] request;
		pthread_exit(NULL);
	}
	struct sockaddr_in masteraddr;
	bzero(&masteraddr, sizeof(masteraddr));
	masteraddr.sin_family = AF_INET;
	masteraddr.sin_port = htons(master_port);
	inet_pton(AF_INET, master_ip, &(masteraddr.sin_addr));
	connect(master_fd, (struct sockaddr*)&masteraddr, sizeof(masteraddr));
	write(master_fd, local_address.c_str(), strlen(local_address.c_str()));
	char master_echo[100];
	read(master_fd, master_echo, 100);

	/*operate according to verb and command*/
	if(http_verb == "GET"){
		// go back to the website welcome page, may be rendered after logout or registration
		if(command == "/welcome.html"){
			//if there is cookie, clear the cookie and memory information
			if(cookie != "NULL"){
				string logout_req = "LOGOUT " + cookie;
				write(master_fd, logout_req.c_str(), strlen(logout_req.c_str()));
				char logout_res[200];
				int bytes = read(master_fd, logout_res, 200);
				logout_res[bytes] = '\0';
				string response(logout_res);
				string can_logout = response.substr(0, response.find(" "));
				string info = response.substr(response.find(" ")+1);
				if(can_logout == "+OK"){ //logout successfully
					user_current_dir.erase(cookie);
					user_files.erase(cookie);
					user_emails.erase(cookie);
					render_page_with_info(fd, "welcome.html", "User logout successfully", "NULL");
				} else {
					render_page_with_info(fd, "welcome.html", info, "");
				}
			} else { // no cookie information is available
				render_page_with_info(fd, "welcome.html", "", "");
			}

		} else if(command == "/register.html"){
			render_page_with_info(fd, "register.html", "", "");
		} else if(command == "/setting.html"){
			render_page_with_info(fd, "setting.html", "", "");
		} else if(command == "/home.html"){
			if(user_current_dir[cookie] != ""){
				user_current_dir[cookie] = "";
			}
			render_page_with_info(fd, "home.html", "Hi " + cookie + "!", "");
		} else if(command == "/emails"){
			string username = cookie;
			string email_view = "VIEW " + username + "#" + "EMAIL";
			write(master_fd, email_view.c_str(), strlen(email_view.c_str()));
			char master_res[8192];
			int bytes = read(master_fd, master_res, 8192);
			master_res[bytes] = '\0';
			string res(master_res);
			res = res.substr(res.find(" ")+1);
			user_emails[username] = get_file_view(res);
			render_dynamic_email_page(fd, username);
		} else if(starts_with(command, "/drive")){ //open the drive service main page
			string username = cookie;
			string drive_view = "VIEW " + username + "#" + "DRIVE";
			write(master_fd, drive_view.c_str(), strlen(drive_view.c_str()));
			char master_res[8192];
			int bytes = read(master_fd, master_res, 8192);
			master_res[bytes] = '\0';
			string res(master_res);
			res = res.substr(res.find(" ")+1);
			user_files[username] = get_file_view(res);
			user_current_dir[cookie] = "";
			render_dynamic_drive_page(fd, cookie);

		} else if(command == "/index"){
			if(cookie == "NULL"){
				render_page_with_info(fd, "welcome.html", "", "");
			} else {
				render_page_with_info(fd, "home.html", "Hi " + cookie + "!", "");
			}
		} else if(command == "/goback"){
			if(user_current_dir[cookie] == ""){
				render_page_with_info(fd, "home.html", "Hi " + cookie + "!", "");
			} else {
				user_current_dir[cookie] = goback_dir(user_current_dir[cookie]);
				render_dynamic_drive_page(fd, cookie);
			}
		} else if(command == "/favicon.ico"){
			fprintf(stderr, "Receive icon request from client  %s:%d fd:[%d]\n", client_ip, client_port, fd);//print out icon request
		} else if(starts_with(command, "/download")) {
			string file_name = command.substr(command.find_last_of("/")+1);
			string master_req = "GET " + cookie + "#" + user_current_dir[cookie] + file_name + "#DRIVE";  
			write(master_fd, master_req.c_str(), strlen(master_req.c_str()));
			char master_res[200];
			int bytes = read(master_fd, master_res, 200);
			master_res[bytes] = '\0';
			string res(master_res);
			string status = res.substr(0, res.find(" "));
			string message = "";
			if(status == "+OK"){
				string storage_addr = res.substr(res.find(" ")+1);
				string storage_res = get_storage_response(storage_addr, master_req);
				string storage_status = storage_res.substr(0, storage_res.find(" "));
				string storage_content = storage_res.substr(storage_res.find(" ") + 1);
				if(storage_status == "+OK"){
					message = storage_content;
				}
				//message = test_file;
			}
			send_file(fd, message, file_name);
		} else if(command == "/cancel") {
			render_dynamic_email_page(fd, cookie);
		} else if(command == "/email.html"){
			render_page_with_info(fd, "email.html", "", "");
		} 
	
	} else if(http_verb == "POST"){
		//deal with user login
		if(command == "/login"){			
			string username = rest.substr(rest.find("user=")+5, rest.find_last_of("&")-rest.find("user=")-5);
			string password = rest.substr(rest.find_last_of("&")+6);
			string login_req = "LOGIN " + username + "#" + password;
			write(master_fd, login_req.c_str(), strlen(login_req.c_str()));
			char master_res[200];
			int bytes = read(master_fd, master_res, 200);
			master_res[bytes] = '\0';
			string res(master_res);
			string status = res.substr(0, res.find(" "));
			string info = res.substr(res.find(" ")+1);
			if(status == "+OK"){
				render_page_with_info(fd, "home.html", "Hi " + username + "!", username);
			} else {
				render_page_with_info(fd, "welcome.html", info, "");
			}
		} else if(command == "/register"){
			size_t user_start = rest.find("user=")+5;
			size_t pass_start = rest.find("pass=")+5;
			size_t confirm_pass = rest.find_last_of("&");
			string username = rest.substr(user_start, pass_start-user_start-6);
			string password = rest.substr(pass_start, confirm_pass-pass_start);
			string confirm_password = rest.substr(confirm_pass+14);

			if(password != confirm_password){
				render_page_with_info(fd, "register.html", "Password does not match", "");
			} else {
				string register_req = "REG " + username + "#" + password;
				write(master_fd, register_req.c_str(), strlen(register_req.c_str()));
				char master_res[200];
				int bytes = read(master_fd, master_res, 200);
				master_res[bytes] = '\0';
				string res(master_res);
				string status = res.substr(0, res.find(" "));
				string info = res.substr(res.find(" ")+1);
				
				if(status == "+OK"){
					render_page_with_info(fd, "welcome.html", "User registration successful.", "");
				} else {
					render_page_with_info(fd, "register.html", info, "");
				}
			}
		} else if(command == "/pass"){
			string username = cookie;
			string password = rest.substr(rest.find("pass=")+5);
			string pass_req = "PASS " + username + "#" + password;
			write(master_fd, pass_req.c_str(), strlen(pass_req.c_str()));
			char master_res[200];
			int bytes = read(master_fd, master_res, 200);
			master_res[bytes] = '\0';
			string res(master_res);
			string status = res.substr(0, res.find(" "));
			string info = res.substr(res.find(" ")+1);

			if(status == "+OK"){
				render_page_with_info(fd, "welcome.html", "Password have been changed successfully", "NULL");
			} else {
				render_page_with_info(fd, "setting.html", info, "");
			}
		} else if(command == "/newfile"){

			vector<string> file_info = parse_upload(request);
			string file_name = file_info[0];
			string file_content = file_info[1];			

			string master_req = "PUT " + cookie + "#" + user_current_dir[cookie] + file_name + "#DRIVE";
			write(master_fd, master_req.c_str(), strlen(master_req.c_str()));
			char master_res[200];
			int bytes = read(master_fd, master_res, 200);
			master_res[bytes] = '\0';
			string res(master_res);
			string status = res.substr(0, res.find(" "));
			vector<string> storage_addr = get_storage_address(res.substr(res.find(" ")+1));

			if(status == "+OK"){
				string storage_res = "-ERR";
				for(int i = 0; i < storage_addr.size(); i++){
					string temp = get_storage_response(storage_addr[i], master_req+"#"+file_content);
					if(temp == "+OK") storage_res = temp;					
				}
				if(storage_res == "+OK"){
					string curr = user_current_dir[cookie];
					user_files[cookie][curr+file_name] = "DRIVE";
				}
				//test_file = file_content;
				string curr = user_current_dir[cookie];
				user_files[cookie][curr+file_name] = "DRIVE";
			}
			render_dynamic_drive_page(fd, cookie);

		} else if(command == "/newfolder"){			
			string folder_name = rest.substr(rest.find("folder=")+7);
			string master_req = "DIR " + cookie + "#" + user_current_dir[cookie] + folder_name + "#FOLDER";  
			write(master_fd, master_req.c_str(), strlen(master_req.c_str()));
			char master_res[200];
			int bytes = read(master_fd, master_res, 200);
			master_res[bytes] = '\0';
			string res(master_res);
			string status = res.substr(0, res.find(" "));
			vector<string> storage_addr = get_storage_address(res.substr(res.find(" ")+1));
			if(status == "+OK"){
				string storage_res = "-ERR";
				for(int i = 0; i < storage_addr.size(); i++){
					string temp = get_storage_response(storage_addr[i], master_req);
					if(temp == "+OK") storage_res = temp;				
				}
				if(storage_res == "+OK"){
					string curr = user_current_dir[cookie];
					user_files[cookie][curr+folder_name] = "FOLDER";
				}
				//string curr = user_current_dir[cookie];
				//user_files[cookie][curr+folder_name] = "FOLDER";
			}
			render_dynamic_drive_page(fd, cookie);
		} else if (command == "/openfolder"){
			string index = rest.substr(rest.find("\r\n\r\n"));
			string folder_name = index.substr(4, index.find("=")-4);
			user_current_dir[cookie] += (folder_name + "/");
			//fprintf(stderr, "%s\n", user_current_dir[cookie].c_str());
			render_dynamic_drive_page(fd, cookie);
		} else if(command == "/deletefolder"){
			string index = rest.substr(rest.find("\r\n\r\n"));
			string folder_name = index.substr(4, index.find("=")-4);
			string del_folder = (user_current_dir[cookie] + folder_name);
			//print_files(user_files[cookie]);
			delete_folder(del_folder, cookie, master_fd);
			//print_files(user_files[cookie]);
			render_dynamic_drive_page(fd, cookie);
		} else if(command == "/deletefile"){
			string index = rest.substr(rest.find("\r\n\r\n"));
			string file_name = index.substr(4, index.find("=")-4);
			string del_file = (user_current_dir[cookie] + file_name);
			//print_files(user_files[cookie]);
			delete_file(del_file, cookie, master_fd);
			//print_files(user_files[cookie]);
			render_dynamic_drive_page(fd, cookie);
		} else if(command == "/deleteemail"){
			string index = rest.substr(rest.find("\r\n\r\n"));
			string email_name = index.substr(4, index.find("=")-4);
			//print_files(user_files[cookie]);
			delete_email(email_name, cookie, master_fd);
			//print_files(user_files[cookie]);
			render_dynamic_email_page(fd, cookie);
		} else if(command == "/movefile"){
			string index = rest.substr(rest.find("\r\n\r\n"));
			string file = index.substr(4, index.find("=")-4);
			string folder = index.substr(index.find("=")+1);
			//print_files(user_files[cookie]);
			move_file(file, folder, cookie, master_fd);
			//print_files(user_files[cookie]);
			render_dynamic_drive_page(fd, cookie);
		} else if(command == "/renamefile"){
			string index = rest.substr(rest.find("\r\n\r\n"));
			string original = index.substr(4, index.find("=")-4);
			string modified = index.substr(index.find("=")+1);
			//print_files(user_files[cookie]);
			rename_file(original, modified, cookie, master_fd);
			//print_files(user_files[cookie]);
			render_dynamic_drive_page(fd, cookie);
		} else if(command == "/renamefolder"){
			string index = rest.substr(rest.find("\r\n\r\n"));
			string original = index.substr(4, index.find("=")-4);
			string modified = index.substr(index.find("=")+1);
			//print_files(user_files[cookie]);
			rename_folder(original, modified, cookie, master_fd);
			//print_files(user_files[cookie]);
			render_dynamic_drive_page(fd, cookie);
		} else if(command == "/sendemail" || command == "/reply" || command == "/forward") {
			string index = rest.substr(rest.find("\r\n\r\n")+4);
			string email_address = index.substr(3, index.find("\r\n")-3);
			string title_start = index.substr(index.find("\r\n")+2);
			string title = title_start.substr(6, title_start.find("\r\n")-6);
			string content_start = title_start.substr(title_start.find("\r\n")+2);
			string content = content_start.substr(8);
			// fprintf(stderr, "email address: %s\n", email_address.c_str());
			// fprintf(stderr, "title: %s\n", title.c_str());
			// fprintf(stderr, "content: %s\n", content.c_str());

			string master_req = "SEND " + cookie + "#" + email_address;  
			write(master_fd, master_req.c_str(), strlen(master_req.c_str()));
			char master_res[200];
			int bytes = read(master_fd, master_res, 200);
			master_res[bytes] = '\0';
			string res(master_res);

			string status = res.substr(0, res.find(" "));
			string info = res.substr(res.find(" ")+1);
			if(status == "+OK"){
				string email_type = info.substr(0, info.find("#"));
				string addr = info.substr(info.find("#")+1);
				if(email_type == "LOCAL"){
					string receiver = email_address.substr(0, email_address.find("@"));
					string storage_request = "PUT " + receiver + "#" + title + "-"+ cookie + "#" + "EMAIL" + "#" + content;
					string storage_res = "-ERR";
					vector<string> storage_addr = get_storage_address(addr);
					for(int i = 0; i < storage_addr.size(); i++){
						string temp = get_storage_response(storage_addr[i], storage_request);
						if(temp == "+OK") storage_res = temp;
					}

					if(storage_res == "+OK"){
						if(cookie+"@penncloud.com" == email_address){
							user_emails[cookie][title+"-"+cookie] = "EMAIL";
						}
				 		render_dynamic_email_page(fd, cookie);
					} else {
						render_page_with_info(fd, "email.html", storage_res, "");
					}
					//render_dynamic_email_page(fd, cookie);
				} else {
					string smtp_request = cookie + "@penncloud.com" + "#" + email_address + "#" + title + "#" + content + "\n";
					string smtp_res = smtp_send_email(addr, smtp_request);
					if(smtp_res.find("+OK") == 0){
						render_dynamic_email_page(fd, cookie);
					} else {
						render_page_with_info(fd, "email.html", "Error sending the email to the remote webmail", "");
					}
					//render_dynamic_email_page(fd, cookie);
				}
			} else {
				render_page_with_info(fd, "email.html", info, "");
			}

		} else if(command == "/reademail" || command == "/forwardemail" || command == "/replyemail"){
			string index = rest.substr(rest.find("\r\n\r\n"));
			string email_name = index.substr(4, index.find("=")-4);
			string master_req = "GET " + cookie + "#" + email_name + "#EMAIL";
			write(master_fd, master_req.c_str(), strlen(master_req.c_str()));
			char master_res[200];
			int bytes = read(master_fd, master_res, 200);
			master_res[bytes] = '\0';
			string res(master_res);
			string status = res.substr(0, res.find(" "));
			string message = "adsk\r\naksdf\r\n";//test for email
			if(status == "+OK"){
				string storage_addr = res.substr(res.find(" ")+1);
				string storage_res = get_storage_response(storage_addr, master_req);
				string storage_status = storage_res.substr(0, storage_res.find(" "));
				string storage_content = storage_res.substr(storage_res.find(" ") + 1);
				if(storage_status == "+OK"){
					message = storage_content;
					if(command == "/reademail"){
						open_email(fd, message, email_name);
					} else if(command == "/forwardemail"){
						forward_email(fd, message, email_name, cookie);
					} else if(command == "/replyemail"){
						reply_email(fd, message, email_name);
					}
				} else {
					render_dynamic_email_page(fd, cookie);
				}

				// if(command == "/reademail"){
				// 	open_email(fd, message, email_name);
				// } else if(command == "/forwardemail"){
				// 	forward_email(fd, message, email_name, cookie);
				// } else if(command == "/replyemail"){
				// 	reply_email(fd, message, email_name);
				// }				

			} else {
				render_dynamic_email_page(fd, cookie);
			}
		} 

	} else if(http_verb == "HEAD"){
		if(command == "shutdown"){
			string ack = notify_balancer(false);
			write(fd, ack.c_str(), strlen(ack.c_str()));
			fprintf(stderr, "Client  %s:%d fd:[%d] terminates\n", client_ip, client_port, fd);
			fprintf(stderr, "The server is shutdown by admin console!\n");
			free(c);
			delete[] request;
			close(fd);
			close(master_fd);
			exit(1);
		}
	}

	/*close the file descriptors and free the resource when terminate*/
	fprintf(stderr, "Client  %s:%d fd:[%d] terminates\n", client_ip, client_port, fd);
	free(c);
	delete[] request;
	close(fd);
	close(master_fd);
	pthread_exit(NULL);
}

int main(int argc, char *argv[]){

	int ch;
	int index;
	string config;
	/*parse the command line arguments*/
	while((ch = getopt(argc, argv, "m:c:n:b:")) != -1){
		switch(ch){
			case 'm':
				master_port = atoi(optarg);
				break;
			case 'c':
				config = optarg;
				break;
			case 'n':
				index = atoi(optarg);
				break;
			case 'b':
				balancer_port = atoi(optarg);
				break;
			default:
    			exit(1);
		}
	}
	if(argc != 9){
    	cout << "Usage: ./WebServer -m [Master Port] -c [ConfigFilePath] -n [server index] -b [balancer port]" << endl;
    	exit(1);
    }

    local_address = get_local_address(index, config);
	int port_number = stoi(local_address.substr(local_address.find(":")+1));
	if (port_number <= 0 || port_number >= 65535) {
    	fprintf(stderr, "Please specify a port number in range 0 - 65535");
    	exit(1);
  	}

  	/*create server socket*/
	int listen_fd  = socket(PF_INET, SOCK_STREAM, 0);
	if(listen_fd  < 0){
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		exit(1);
	}
	struct sockaddr_in servaddr;
	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htons(INADDR_ANY);
	servaddr.sin_port = htons(port_number);
	int can_bind = bind(listen_fd, (struct sockaddr*)&servaddr, sizeof(servaddr));
	if(can_bind != 0){
		fprintf(stderr, "Socket is not successfully bind, please wait and restart.\n");
		exit(1);
	}
	listen(listen_fd, 500);
	notify_balancer(true);

	/*connectionless communication with each web request*/
	while(true){
		struct sockaddr_in clientaddr;
		socklen_t clientaddrlen = sizeof(clientaddr);
		int fd = accept(listen_fd, (struct sockaddr*)&clientaddr, &clientaddrlen);//accept connection from web browser

		struct client *c = (struct client*)malloc(sizeof(struct client*));
		c->fd = fd;
		c->port = ntohs(clientaddr.sin_port);
		c->ip = inet_ntoa(clientaddr.sin_addr);
		fprintf(stderr, "New connection from client %s:%d fd:[%d]\n", c->ip, c->port, fd);
		pthread_t thread;
		pthread_create(&thread, NULL, worker, c); //assign a new worker thread for dealing with command
	}
	return 0;
}
