
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
#include <map>
#include <unordered_map>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iomanip>
using namespace std;

// GET state
vector<vector<string> > DATA;
vector<string> DATA_ALL;
string req_dummy = "1#1#0#1#0#1";
int PORT;
// get states of front end servers from load balancer
int LOAD_BALANCER_PORT = 8000;
char LOAD_BALANCER_IP[] = "127.0.0.1";
// get states of back end servers from master
int MASTER_PORT = 5000;
char MASTER_IP[] = "127.0.0.1";
string HTTP_CONFIG = "http_config.txt";
string STORAGE_CONFIG = "config";

vector<string> frontend_servers;
vector<string> backend_servers;
map<string, int> frontend_state;
map<string, int> backend_state;

/*client information*/
struct client{
	int fd;
	int port;
	char* ip;
};

/*detect if the file name start with the prefix*/
bool starts_with(string content, string prefix){
	return content.substr(0, prefix.size()) == prefix;
}


vector<string> extract_table_data(string msg, char breaking_siign){
	// remove +OK
	vector<string> results;
	size_t found = msg.find(" ");
	// if(found == string::npos){
	// 	results.push_back("EMPTY");
	// 	results.push_back("EMPTY");
	// 	results.push_back("EMPTY");
	// 	results.push_back("EMPTY");
	// 	return results;
	// }
	msg = msg.substr(found+1);
	
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


/*parse the file and directory from the message string returned by master node*/
vector<int> extract_state(string msg, char breaking_siign){
	// remove +OK
	size_t found = msg.find(" ");
	msg = msg.substr(found+1);
	vector<int> results;
	int index = 0;
	int last_index = 0;
	int state;
	for (; index < msg.size(); index++){
		if (msg.at(index) == breaking_siign){
			state = stoi(msg.substr(last_index, index - last_index));
			cout << state;
			results.push_back(state);
			last_index = index + 1;
		}
	}
	state = stoi(msg.substr(last_index));
	results.push_back(state);
	return results;
}


// ask load balancer for front server states
void get_frontend_state(){
	/*create the socket with the load balancer*/
	int lb_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(lb_fd < 0){	
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		exit(1);
	}	
	struct sockaddr_in load_balancer_addr;
	bzero(&load_balancer_addr, sizeof(load_balancer_addr));
	load_balancer_addr.sin_family = AF_INET;
	load_balancer_addr.sin_port = htons(LOAD_BALANCER_PORT);
	inet_pton(AF_INET, LOAD_BALANCER_IP, &(load_balancer_addr.sin_addr));
	int conn = connect(lb_fd, (struct sockaddr*)&load_balancer_addr, sizeof(load_balancer_addr));
	if (conn != 0){
		fprintf(stderr, "connection failed (%s)\n", strerror(errno));
		exit(1);
	}

	// ask state of frot end servers
	string state_request = "GET /state";
	write(lb_fd, state_request.c_str(), strlen(state_request.c_str()));
	char lb_res[1000];
	int bytes = read(lb_fd, lb_res, 1000);
	lb_res[bytes] = '\0';
	string res(lb_res);

	string req = res;
	vector<int> states = extract_state(req, '#');
	cout << "depacked" << endl;
	for (int i = 0; i < frontend_servers.size(); i++){
		if (states.at(i) == 1) frontend_state[frontend_servers.at(i)] = 1;
		else frontend_state[frontend_servers.at(i)] = 0;
	}
	close(lb_fd);
}

// ask load balancer for front server states
void get_backend_state(){
	/*create the socket with the load balancer*/
	int master_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(master_fd < 0){	
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		exit(1);
	}	
	struct sockaddr_in master_addr;
	bzero(&master_addr, sizeof(master_addr));
	master_addr.sin_family = AF_INET;
	master_addr.sin_port = htons(MASTER_PORT);
	inet_pton(AF_INET, MASTER_IP, &(master_addr.sin_addr));
	int conn = connect(master_fd, (struct sockaddr*)&master_addr, sizeof(master_addr));
	if (conn != 0){
		fprintf(stderr, "connection failed (%s)\n", strerror(errno));
		exit(1);
	}

	// ask state of frot end servers
	string id;
	stringstream ss;
	ss << PORT;
	id = "127.0.0.1:" + ss.str();
	write(master_fd, id.c_str(), strlen(id.c_str()));
	char id_buf[10];
	read(master_fd, id_buf, 10);

	string state_request = "STATE";
	write(master_fd, state_request.c_str(), strlen(state_request.c_str()));
	char master_res[1000];
	int bytes = read(master_fd, master_res, 1000);
	master_res[bytes] = '\0';
	string res(master_res);

	string req = res;
	vector<int> states = extract_state(req, '#');
	cout << "depacked" << endl;

	for (int i = 0; i < backend_servers.size(); i++){
		// cout << states.at(i) << endl;
		if (states.at(i) == 1) {
			// cout << "ha" << endl;
			backend_state[backend_servers.at(i)] = 1;
		}else backend_state[backend_servers.at(i)] = 0;
	}
	close(master_fd);
}


string read_html_to_string(string dir){
	ifstream file(dir.c_str());
    string line;
    string html;
    while (getline(file, line)) html += line;
    file.close();
    return html;
}

string get_frontend_view(string id, int state){
	// split ip and port
	string ip;
	string port;
	for (int i = 0; i < id.size(); i++){
		if (id.at(i) == ':'){
			ip = id.substr(0, i);
			port = id.substr(i+1);
		}
	}
	string view;
	string tag_li = "<li class=\"list-group-item list-group-item-";
	string tag_state; // success
	string tag_li_2 = "\">";
	string server_id = id;
	string href = "admin/" + port;;
	string tag_content;
	string p;
	string tag_a_2 = "</p></li>";
	// construct the rest information
	if (state == 1){
		tag_content = "<a class=\"pull-right\" href=\"";
		tag_content += id;
		tag_content += "\" class=\"btn btn-success\">Shut down</a>";
		tag_state = "success";
		p = "";
	}else{
		tag_content = "<p class=\"pull-right\">";
		tag_state = "danger";
		p = "Server down";		
	}
	view = tag_li + tag_state + tag_li_2 + server_id + tag_content + p + tag_a_2;
	return view;
}


string get_backend_view(string id, int state){
	return get_frontend_view(id, state);
}


void render_home_page(int fd) {

	// header
	string status = "HTTP/1.1 200 OK\r\n";
	string length = "Content-Length: ";
	string type = "Content-Type: text/html\r\n";

	// top session of html
	string dir = "html/admin_up.html";
	string PAGE_UP = read_html_to_string(dir);
	string PAGE_BOTTOM = "</body></html>";

	// front end servers
	string frontend_content =  "<div class=\"container\">";
	frontend_content +=  "<div class=\"row\">";
	frontend_content +=  "<div class=\"col-sm-6\">";
	frontend_content +=  "<h3>Frontend server</h3>";
	frontend_content +=  "<ul class=\"list-group\">";
	string frontend_content_end = "</ul></div></div></div>";

	string frontend_server_content;
	for (int i = 0; i < frontend_servers.size(); i++){
		string id = frontend_servers.at(i);
		int state = frontend_state[id];
		frontend_server_content += get_frontend_view(id, state);
	}
	string frontend = frontend_content + frontend_server_content + frontend_content_end;

	// back end servers
	string backend_content =  "<div class=\"container\">";
	backend_content +=  "<div class=\"row\">";
	backend_content +=  "<div class=\"col-sm-6\">";
	backend_content +=  "<h3>Backend server</h3>";
	backend_content +=  "<ul class=\"list-group\">";
	string backend_content_end = "</ul></div></div></div>";

	string backend_server_content;
	for (int i = 0; i < backend_servers.size(); i++){
		string id = backend_servers.at(i);
		int state = backend_state[id];
		backend_server_content += get_frontend_view(id, state);
	}
	string backend = backend_content + backend_server_content + backend_content_end;	

	string data_but_content = "<div class=\"container\">";
	data_but_content += "<a href=\"/data\" type=\"button\" class=\"btn btn-info\">Raw Data Preview</a>";
	data_but_content += "</div>";

	string view = PAGE_UP + frontend + backend + data_but_content + PAGE_BOTTOM;

	stringstream ss;
	ss << view.size();
	string response = status + length + ss.str() + "\r\n" + type + "\r\n" + view;
	write(fd, response.c_str(), response.length());
}

void shutdown_back(int fd, string id){
	string storage_ip = id.substr(0, id.find(":"));
	int storage_port = stoi(id.substr(id.find(":")+1));
	int storage_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(storage_fd < 0){	
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		exit(1);
	}	
	struct sockaddr_in storage_addr;
	bzero(&storage_addr, sizeof(storage_addr));
	storage_addr.sin_family = AF_INET;
	storage_addr.sin_port = htons(storage_port);
	inet_pton(AF_INET, storage_ip.c_str(), &(storage_addr.sin_addr));
	int conn = connect(storage_fd, (struct sockaddr*)&storage_addr, sizeof(storage_addr));
	if (conn != 0){
		fprintf(stderr, "connection failed (%s)\n", strerror(errno));
		exit(1);
	}

	// ask state of frot end servers
	string state_request = "SHUTDOWN";
	string s = "8";
	write(storage_fd, s.c_str(), strlen(s.c_str()));
	char len_buf[30];
	read(storage_fd, len_buf, 30);

	write(storage_fd, state_request.c_str(), strlen(state_request.c_str()));
	// char buf[30];
	// int bytes = read(storage_fd, buf, 30);
	// buf[bytes] = '\0';
	// string res(buf);
	// if (res == "+OK"){
	// 	cout << "shutdown " << id << " successfully" << endl;
	// 	backend_state[id] = 0;
	// }
	cout << "shutdown " << id << " successfully" << endl;
	backend_state[id] = 0;
}

void shutdown_front(int fd, string id){

	string http_ip = id.substr(0, id.find(":"));
	int http_port = stoi(id.substr(id.find(":")+1));
	int http_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(http_fd < 0){	
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		exit(1);
	}	
	struct sockaddr_in http_addr;
	bzero(&http_addr, sizeof(http_addr));
	http_addr.sin_family = AF_INET;
	http_addr.sin_port = htons(http_port);
	inet_pton(AF_INET, http_ip.c_str(), &(http_addr.sin_addr));
	int conn = connect(http_fd, (struct sockaddr*)&http_addr, sizeof(http_addr));
	if (conn != 0){
		fprintf(stderr, "connection failed (%s)\n", strerror(errno));
		exit(1);
	}

	// ask state of frot end servers
	string state_request = "HEAD shutdown";
	write(http_fd, state_request.c_str(), strlen(state_request.c_str()));
	char buf[30];
	int bytes = read(http_fd, buf, 30);
	buf[bytes] = '\0';
	string res(buf);

	if (res == "+OK"){
		cout << "shutdown " << id << " successfully" << endl;
		frontend_state[id] = 0;
	}
}


void shutdown_server(int fd, string command){
	string id = command.substr(1);
	cout << "id: " << id << endl;

	if (frontend_state.find(id) != frontend_state.end()) {
		shutdown_front(fd, id);
	}else {
		shutdown_back(fd, id);
	}
	render_home_page(fd);
}

void render_table(int fd){

	// vector<vector<string> > V;
	// vector<string> v1;
	// vector<string> v2;
	// v1.push_back("test");
	// v1.push_back("test");
	// v1.push_back("test");
	// v1.push_back("test");
	// v2.push_back("test");
	// v2.push_back("test");	
	// v2.push_back("test");	
	// v2.push_back("test");
	// V.push_back(v1);
	// V.push_back(v2);			

    string status = "HTTP/1.1 200 OK\r\n";
	string length = "Content-Length: ";
	string type = "Content-Type: text/html\r\n";
	
	string message_body;
	message_body += "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\" />\n<title>Admin-Data</title>";
	message_body += "<link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css\" integrity=\"sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u\" crossorigin=\"anonymous\">";		
	message_body += "</head><body>\n<center>\n<h1>Data entry preview </h1>\n</center>";

	message_body += "<div class=\"container\"><table class=\"table\"><thead><tr><th>User</th><th>Filename</th><th>Filetype</th><th>Storage Addr</th></tr></thead><tbody>";
	
	for(int i = 0; i < DATA_ALL.size(); i++){

    	string user = DATA_ALL.at(i);
    	string filename = DATA_ALL.at(++i);
    	string filetype = DATA_ALL.at(++i);
    	string server = DATA_ALL.at(++i);

    	message_body += "<tr><td><p>";
    	message_body += user;
    	message_body += "</p></td>";
    	message_body += "<td><p>";
    	message_body += filename;
    	message_body += "</p></td>";
    	message_body += "<td><p>";
    	message_body += filetype;
    	message_body += "</p></td>";
    	message_body += "<td><p>";
    	message_body += server;
    	message_body += "</p></td></tr>";
    }
    message_body += "</tbody></table>";
    message_body += "<div class=\"container\"><a href=\"/admin\">Back</a></div>";
    message_body += "</body>\n</html>\n";

	stringstream ss;
	ss << message_body.size();
	string response = status + length + ss.str() + "\r\n" + type + "\r\n" + message_body;
	write(fd, response.c_str(), response.length());
}


void get_table(int fd){
	int master_fd = socket(PF_INET, SOCK_STREAM, 0);
	if(master_fd < 0){	
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		exit(1);
	}	
	struct sockaddr_in master_addr;
	bzero(&master_addr, sizeof(master_addr));
	master_addr.sin_family = AF_INET;
	master_addr.sin_port = htons(MASTER_PORT);
	inet_pton(AF_INET, MASTER_IP, &(master_addr.sin_addr));
	int conn = connect(master_fd, (struct sockaddr*)&master_addr, sizeof(master_addr));
	if (conn != 0){
		fprintf(stderr, "connection failed (%s)\n", strerror(errno));
		exit(1);
	}
	// ask state of frot end servers
	string table = "TABLE";
	string id;
	stringstream ss;
	ss << PORT;
	id = "127.0.0.1:" + ss.str();
	write(master_fd, id.c_str(), strlen(id.c_str()));
	char id_buf[10];
	read(master_fd, id_buf, 10);

	string state_request = "TABLE";
	write(master_fd, state_request.c_str(), strlen(state_request.c_str()));
	char master_res[1000];
	int bytes = read(master_fd, master_res, 1000);
	master_res[bytes] = '\0';
	string res(master_res);

	DATA_ALL = extract_table_data(res, '#');
	render_table(fd);

}


void* admin_console(void* arg) {

	struct client *c = (struct client *)arg;
	int fd = c->fd;
	int client_port = c->port;
	char* client_ip = c->ip;

	char request[6000];
	int read_bytes = read(fd, request, sizeof(request));

	/*check connection error or termination*/
	if(read_bytes <= 0){
		if(read_bytes < 0){
			fprintf(stderr, "Error when reading from socket (%s)\n", strerror(errno));
		} else {
			fprintf(stderr, "Client  %s:%d fd:[%d] terminates\n", client_ip, client_port, fd);
		}
		free(c);
		close(fd);
		pthread_exit(NULL);
	}

	request[read_bytes] = '\0';
	string req(request);
	string http_verb = req.substr(0, req.find(" "));
	string rest = req.substr(req.find(" ")+1);
	string command = rest.substr(0, rest.find(" "));
	cout << "request: \n" << req << endl;
	cout << http_verb << " " << command << endl;	

	if (command == "/admin") {
		get_frontend_state();
		get_backend_state();
		render_home_page(fd);
	}
	else if (command == "/refresh") {
		get_frontend_state();
		get_backend_state();
		render_home_page(fd);
	} 
	else if (starts_with(command, "/127.0.0.1")){
		shutdown_server(fd, command);
	}
	else if (command == "/data"){
		get_table(fd);
	}	 

	free(c);
	close(fd);
	pthread_exit(NULL);	
}


void config_backend_server(){
	ifstream infile;
	infile.open(STORAGE_CONFIG);
	if(!infile.is_open()){
		fprintf(stderr, "The storage server file could not be opened.\n");
		infile.close();
		exit(1);
	}
	string line;
	while(getline(infile, line)){
		size_t found = line.find(", ");
		if (found >= line.size()) continue;
		backend_servers.push_back(line.substr(0, found));
		backend_servers.push_back(line.substr(found+2));
	}
	infile.close();
	for (int i = 0; i < backend_servers.size(); i++){
		backend_state.insert(make_pair(backend_servers.at(i), 0));
	}
	cout << "storage servers" << endl;
	for (int i = 0; i < backend_servers.size(); i++){
		cout << backend_servers.at(i) << endl;
	}
}

void config_frontend_server(){

	ifstream infile;
	infile.open(HTTP_CONFIG);
	if(!infile.is_open()){
		fprintf(stderr, "The http server config file could not be opened.\n");
		infile.close();
		exit(1);
	}
	string line;
	while(getline(infile, line)){
		frontend_servers.push_back(line);
	}
	infile.close();
	for (int i = 0; i < frontend_servers.size(); i++){
		frontend_state.insert(make_pair(frontend_servers.at(i), 0));
	}
	cout << "http servers" << endl;
	for (int i = 0; i < frontend_servers.size(); i++){
		cout << frontend_servers.at(i) << endl;
	}				
}


int main(int argc, char *argv[]){

	int ch;
	int index;
	string config;
	/*parse the command line arguments*/
	while((ch = getopt(argc, argv, "l:p:m:")) != -1){
		switch(ch){
			case 'l':
				LOAD_BALANCER_PORT = atoi(optarg);
				break;
			case 'p':
				PORT = atoi(optarg);
				break;
			case 'm':
				MASTER_PORT = atoi(optarg);
				break;
			default:
    			exit(1);
		}
	}
	if(argc != 7){
    	cout << "Usage: ./admin -l [load balancer port] -p [admin Port] -m [master port]" << endl;
    	exit(1);
    }

  	// create server socket*/
	int listen_fd  = socket(PF_INET, SOCK_STREAM, 0);
	if(listen_fd  < 0){
		fprintf(stderr, "Can not open socket (%s)\n", strerror(errno));
		exit(1);
	}

	// config server sock
	struct sockaddr_in servaddr;
	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htons(INADDR_ANY);
	servaddr.sin_port = htons(PORT);

	cout << "binding on port: " << PORT << endl;
	// bind
	int can_bind = bind(listen_fd, (struct sockaddr*)&servaddr, sizeof(servaddr));
	if(can_bind != 0){
		fprintf(stderr, "Socket is not successfully bind, please wait and restart.\n");
		exit(1);
	}	
	listen(listen_fd, 500);

	// store server ip addr from config file
	config_frontend_server();
	config_backend_server();

	while (true) {
		struct sockaddr_in clientaddr;
		socklen_t clientaddrlen = sizeof(clientaddr);
		int fd = accept(listen_fd, (struct sockaddr*)&clientaddr, &clientaddrlen);//accept connection from web browser
		
		struct client *c = (struct client*)malloc(sizeof(struct client*));
		c->fd = fd;
		c->port = ntohs(clientaddr.sin_port);
		c->ip = inet_ntoa(clientaddr.sin_addr);
		fprintf(stderr, "New connection from client %s:%d fd:[%d]\n", c->ip, c->port, fd);

		pthread_t thread;
		pthread_create(&thread, NULL, admin_console, c);
	}
	close(listen_fd);
	return 0;
}

