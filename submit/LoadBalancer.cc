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
using namespace std;

int port;
int admin_port;
char ip[] = "127.0.0.1";
int round_robin = 0;
vector<string> httpservers;
unordered_map<string, bool> httpserver_status;
pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

/*client information*/
struct client{
	int fd;
	int port;
	char* ip;
};

bool check_httpservers(){
	pthread_mutex_lock(&mtx);
	for(int i = 0; i < httpservers.size(); i++){
		if(httpserver_status[httpservers[i]]){
			pthread_mutex_unlock(&mtx);
			return true;
		}
	}
	pthread_mutex_unlock(&mtx);
	return false;
}

void config_httpservers(string config){
	ifstream infile;
	infile.open(config);
	if(!infile.is_open()){
		fprintf(stderr, "The file could not be opened.\n");
		infile.close();
		exit(1);
	}
	string line;
	while(getline(infile, line)){
		if(line != ""){
			httpservers.push_back(line);
			httpserver_status[line] = false;
		}
	}
	infile.close();
}

void print_http_servers(){
	cout << "Http server status:" << endl;
	for(int i = 0; i < httpservers.size(); i++){
		cout << httpservers[i] << "  " << httpserver_status[httpservers[i]] << endl;
	}
	return;
}

void redirect_adminpage(int fd){
	
	string header = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: ";
	string html_1 = "<!DOCTYPE html><html><head><title>Redirect</title><link rel=\"icon\" href=\"data:;base64,=\"><meta http-equiv=\"refresh\" content=\"0; URL='http://";
	string html_2 = "'\"/></head></html>";
	string content, response;
	string addr = "localhost:" + to_string(admin_port);
	string id = addr + "/admin";
	content = html_1 + id + html_2;
	stringstream ss;
	ss << content.size();
	string conlen = ss.str();
	response += header;
	response += conlen;
	response += "\r\n\r\n";
	response += content;

	fprintf(stderr, "Redirect the request to %s\n", id.c_str());
	write(fd, response.c_str(), strlen(response.c_str()));
}

void redirect_welcomepage(int fd){
	
	pthread_mutex_lock(&mtx);
	while(httpserver_status[httpservers[round_robin]] == false){
		round_robin = (round_robin+1) % httpservers.size();
	}
	string addr = httpservers[round_robin];
	round_robin = (round_robin+1) % httpservers.size();
	pthread_mutex_unlock(&mtx);

	string header = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: ";
	string html_1 = "<!DOCTYPE html><html><head><title>Redirect</title><link rel=\"icon\" href=\"data:;base64,=\"><meta http-equiv=\"refresh\" content=\"0; URL='http://";
	string html_2 = "'\"/></head></html>";
	string content, response;
	string id = addr + "/index";
	content = html_1 + id + html_2;
	stringstream ss;
	ss << content.size();
	string conlen = ss.str();
	response += header;
	response += conlen;
	response += "\r\n\r\n";
	response += content;

	fprintf(stderr, "Redirect the request to %s/index\n", addr.c_str());
	write(fd, response.c_str(), strlen(response.c_str()));
}

void *worker(void *arg){
	struct client *c = (struct client *)arg;
	int fd = c->fd;
	int client_port = c->port;
	char* client_ip = c->ip;

	char* request = new char[4096];
	int read_bytes = read(fd, request, 4096);

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
	/*parse http-verb and command from request*/
	request[read_bytes] = '\0';
	string req(request);
	string http_verb = req.substr(0, req.find(" "));
	string rest = req.substr(req.find(" ")+1);
	string command = rest.substr(0, rest.find(" "));

	if(http_verb == "GET"){
		if(command == "/"){
			if(!check_httpservers()){
				fprintf(stderr, "No available http servers.\n");
			} else {
				redirect_welcomepage(fd);
			}
		} else if(command == "/admin"){
			redirect_adminpage(fd);
		} else if(command == "/state"){
			string admin_res = "+OK ";
			pthread_mutex_lock(&mtx);
			for(int i = 0; i < httpservers.size(); i++){
				if(httpserver_status[httpservers[i]]){
					admin_res += "1";
				} else {
					admin_res += "0";
				}

				if(i < httpservers.size()-1){
					admin_res += "#";
				}
			}
			pthread_mutex_unlock(&mtx);
			cout << admin_res << endl;
			write(fd, admin_res.c_str(), strlen(admin_res.c_str()));
		}
	} else if(http_verb == "POST"){
		if(command == "/start"){
			string server_addr = rest.substr(rest.find(" ")+1);
			pthread_mutex_lock(&mtx);
			httpserver_status[server_addr] = true;
			pthread_mutex_unlock(&mtx); 
			fprintf(stderr, "Http server %s starts.\n", server_addr.c_str());
			print_http_servers();
		} else if(command == "/shutdown") {
			string server_addr = rest.substr(rest.find(" ")+1);
			pthread_mutex_lock(&mtx);
			httpserver_status[server_addr] = false;
			pthread_mutex_unlock(&mtx);
			fprintf(stderr, "Http server %s shuts down.\n", server_addr.c_str());
			print_http_servers();
		}
	}
	fprintf(stderr, "Client  %s:%d fd:[%d] terminates\n", client_ip, client_port, fd);
	free(c);
	close(fd);
	pthread_exit(NULL);
}

int main(int argc, char *argv[]){
	int c;
	string config;
	/*parse the command line arguments*/
	while((c = getopt(argc, argv, "c:p:a:")) != -1){
		switch(c){
			case 'c':
				config = optarg;
				break;
			case 'p':
				port = atoi(optarg);
				break;
			case 'a':
				admin_port = atoi(optarg);
				break;
			default:
    			exit(1);
		}
	}
	if(argc != 7){
    	cout << "Usage: ./LoadBalancer -c [ConfigFilePath] -p [port number] -a [admin console]" << endl;
    	exit(1);
    }
	config_httpservers(config);
	print_http_servers();

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
	servaddr.sin_port = htons(port);
	int can_bind = bind(listen_fd, (struct sockaddr*)&servaddr, sizeof(servaddr));
	if(can_bind != 0){
		fprintf(stderr, "Socket is not successfully bind, please wait and restart.\n");
		exit(1);
	}
	listen(listen_fd, 1000);

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