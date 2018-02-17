#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <string>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <fstream>
#include <ctime>
#include <dirent.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/nameser.h>
#include <resolv.h>
#include <netdb.h>
#include <openssl/md5.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <fcntl.h>

using namespace std;

const int NUMBER_MAX = 10000;
int all_fd_array[NUMBER_MAX] = {};

struct Mail_Content {
	string sender;
	string receiver;
	string content;
	int fd;
};

// write response to the client
bool do_write(int fd, char *buf, int len) {
	int sent = 0;
	while (sent < len) {
		int n = write(fd, &buf[sent],len - sent);
		if (n < 0) {
			fprintf(stderr, "Error in writing response!");
			return false;
		}
		sent += n;
	}
	return true;
}

// The CTRL-C handler.
void my_handler(int s) {
	string shut_down_message = "-ERR Server shutting down\r\n";

	for (int i = 0; i < NUMBER_MAX; i++) {
		if (all_fd_array[i] == 1) {
			// write the message and close the connection.
			do_write(i, &shut_down_message.at(0), shut_down_message.size());
			all_fd_array[i] = 0;
			close(i);
		}
	}
	// !!!exit the server.
	exit(1);
}

// check whether need to output the debug message.
bool debug_flag = false;


// print the debug messages according to the input.
void print_debug_message(int fd, string message) {
	fprintf(stderr, "[%d] %s", fd, message.c_str());
}



bool do_read(int fd, char *buf, int len) {
	int rcvd = 0;
	while (rcvd < len) {
		int n = read(fd, &buf[rcvd], len-rcvd);
		if (n<0)
			return false;
		rcvd += n;
	}
	return true;
}


bool ip_valid(string ip) {
	size_t pos = ip.find(".");
	if (pos == 3) return false;
	else return true;
}

void * worker_thread(void * params) {


	int *fd_pointer = (int*) params;
	int comm_fd = *fd_pointer;
	delete fd_pointer;

	// if needed, show the debug message.
	if (debug_flag) print_debug_message(comm_fd, "New connection\r\n");


	// Then read the message from the client.
	// using a buffer for each connection.
	int BUFFER_SIZE = 100;
	char buffer[BUFFER_SIZE];
	// using stringstream to store the temp command.
	stringstream ss;
	int times = 1;

	while (true) {

		// run the first time.
		if (times > 0) {
			int n = read(comm_fd, buffer, BUFFER_SIZE);
			if (n > 0) {
				buffer[n] = 0;
				ss << buffer;
			}
			times--;
		}

		// using getline() to store the valid command.
		string command;
		getline(ss, command, '\n');

		// when we do not extract the whole line ('\n)
		// or we are at the end of the previous command line.
		if (ss.eof()) {
			ss.clear();
			ss << command;

			// read.
			int n = read(comm_fd, buffer, BUFFER_SIZE);
			if (n > 0) {
				buffer[n] = 0;
				ss << buffer;
			}
			continue;
		}

		// print debug info if needed: the command.
		if (debug_flag) {
			print_debug_message(comm_fd, "C: " + command + "\n");
		}
		// the command should in the format of: sender#receiver#content
		size_t pos1 = command.find("#");
		string sender = command.substr(0, pos1);

		string rest1 = command.substr(pos1 + 1);
		size_t pos2 = rest1.find("#");
		string receiver = rest1.substr(0, pos2);

		string rest2 = rest1.substr(pos2 + 1);
		size_t pos3 = rest2.find("#");
		string subject = rest2.substr(0, pos3);
		string mail_content = rest2.substr(pos3 + 1);


		// get the domain name of the receiver.
		size_t pos = receiver.find("@");
		if (pos == string::npos) {
			if (debug_flag) {
				fprintf(stderr, "The input receiver address is wrong.\n");
			}
			string msg = "-ERR Wrong input receiver address\n";
			do_write(comm_fd, &msg.at(0), msg.size());

			continue;

		}
		string dname = receiver.substr(pos + 1);
		const size_t size = 1024;
		unsigned char buffer[size];

		int result = res_query(dname.c_str(), C_IN, T_MX, buffer, size);


		// if we can get the ip address.
		if (result > 0 && result < static_cast<int>(size)) {

			if (debug_flag) {
				fprintf(stderr, "The host %s can be connected with result value: %d.\n", dname.c_str(), result);
			}
			HEADER *hdr = reinterpret_cast<HEADER*> (buffer);
			if (hdr->rcode != NOERROR) {
				if (debug_flag) {
					fprintf(stderr, "Error when reading the header.\n");
				}
				string msg = "-ERR no corresponding ip address\n";
				do_write(comm_fd, &msg.at(0), msg.size());
				continue;
			}

			int addrrecords = ntohs (hdr->arcount);

			ns_msg m;
			int k = ns_initparse (buffer, result, &m);
			if (k == -1) {
				if (debug_flag) {
					fprintf(stderr, "Error when parsing the buffer.\n");
				}
				string msg = "-ERR no corresponding ip address\n";
				do_write(comm_fd, &msg.at(0), msg.size());
				continue;
			}

			char* ip_address;
			bool has_mx = false;

			// then to get the ip address for the MX
			int j;
			for (j = 0; j < addrrecords; j++) {
				ns_rr rr;
				int temp = ns_parserr(&m, ns_s_ar, j, &rr);
				if (temp == -1) {
					if (debug_flag) fprintf(stderr, "Error when parse the record: %s\n", strerror (errno));
					continue;
				}
				const size_t size = NS_MAXDNAME;
				unsigned char name[size];
				int t = ns_rr_type (rr);
				const u_char *data = ns_rr_rdata (rr);
				if (t == T_MX) {
					int pref = ns_get16 (data);
					ns_name_unpack (buffer, buffer + result, data + sizeof (u_int16_t), name, size);
					char name2[size];
					ns_name_ntop (name, name2, size);
					ip_address = name2;
					has_mx = true;
					if (debug_flag) fprintf(stderr, "In T_MX\n");
					if (ip_valid(ip_address)) break;
				} else if (t == T_A) {
					unsigned int addr = ns_get32 (data);
					struct in_addr in;
					in.s_addr = ntohl (addr);
					char *a = inet_ntoa (in);
					ip_address = a;
					has_mx = true;
					if (debug_flag) fprintf(stderr, "In T_A\n");
					if (ip_valid(ip_address)) break;
				} else if (t == T_NS) {
					ns_name_unpack (buffer, buffer + result, data, name, size);
					char name2[size];
					ns_name_ntop (name, name2, size);
					ip_address = name2;
					has_mx = true;
					if (debug_flag) fprintf(stderr, "In T_NS\n");
					if (ip_valid(ip_address)) break;
				}
			}

			if (j == addrrecords && receiver.find("@gmail.com") != string::npos) {
				char temp[] = "74.125.22.27";
				ip_address = temp;
			}

			if (has_mx) {
				if (debug_flag) {
					fprintf(stderr, "The ip address for host %s is: %s\n", dname.c_str(), ip_address);
				}

				int sockfd = socket(PF_INET, SOCK_STREAM, 0);
				if (sockfd < 0) {
					if (debug_flag) fprintf(stderr, "Cannot open socket (%s)\n", strerror(errno));
					continue;
				}

				if (debug_flag) {
					fprintf(stderr, "New Connection\n");
				}

				// Set non-blocking
				long arg;
				arg = fcntl(sockfd, F_GETFL, NULL);
				arg |= O_NONBLOCK;
				fcntl(sockfd, F_SETFL, arg);

				// connect to the server.
				struct sockaddr_in servaddr;
				bzero(&servaddr, sizeof(servaddr));
				servaddr.sin_family = AF_INET;
				servaddr.sin_port = htons(25);
				int status = inet_pton(AF_INET, ip_address, &(servaddr.sin_addr));
				if (debug_flag) fprintf(stderr, "The status value is: %d\n", status);

				int res = connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));

				fd_set fdset;
				struct timeval tv;
				socklen_t lon;
				int valopt;
				if (res < 0) {
					if (errno == EINPROGRESS) {
						FD_ZERO(&fdset);
						FD_SET(sockfd, &fdset);
						tv.tv_sec = 10;             /* 10 second timeout */
						tv.tv_usec = 0;

						if (select(sockfd+1, NULL, &fdset, NULL, &tv) > 0) {
							lon = sizeof(int);
							getsockopt(sockfd, SOL_SOCKET, SO_ERROR, (void*)(&valopt), &lon);
							if (valopt) {
								string msg = "-ERR Error in connection\n";
								do_write(comm_fd, &msg.at(0), msg.size());

								fprintf(stderr, "Error in connection() %d - %s\n", valopt, strerror(valopt));
								continue;
							}
						} else {
							string msg = "-ERR Error in connection\n";
							do_write(comm_fd, &msg.at(0), msg.size());
							fprintf(stderr, "Timeout or error() %d - %s\n", valopt, strerror(valopt));
							continue;
						}
					}else {
						string msg = "-ERR Error in connection\n";
						do_write(comm_fd, &msg.at(0), msg.size());
						fprintf(stderr, "Error connecting %d - %s\n", errno, strerror(errno));
						continue;
					}
				}
				// Set to blocking mode again...
				arg = fcntl(sockfd, F_GETFL, NULL);
				arg &= (~O_NONBLOCK);
				fcntl(sockfd, F_SETFL, arg);

				if (debug_flag) {
					fprintf(stderr, "Sending data to the ip address: %s\n", ip_address);
				}

				// here we should send helo, mail from, rcpt to and the data to the remote server and wait for the response.
				string helo_temp = "HELO server\r\n";
				string mail_from = "MAIL FROM:<" + sender + ">\r\n";
				string rcpt_to = "RCPT TO:<" + receiver + ">\r\n";
				string data_start = "DATA\r\n";

				string data_to = "To: " + receiver + "\r\n";
				string data_from = "From: " + sender + "\r\n";
				string data_subject = "Subject: " + subject + "\r\n";
				string data_end = "\r\n.\r\n";

				string content = data_to + data_from + data_subject + mail_content + data_end;

				vector<string> requests;
				requests.push_back(helo_temp);
				requests.push_back(mail_from);
				requests.push_back(rcpt_to);
				requests.push_back(data_start);
				requests.push_back(content);

				bool success_sent = true;
				for (int i = 0; i < 5; i++) {
					string curr = requests[i];

					if (debug_flag) {
						cerr << "The data sent is: " << curr << endl;
					}

					bool write = do_write(sockfd, &curr.at(0), curr.size());

					int BUFFER_SIZE = 100;
					char buffer[BUFFER_SIZE];
					// using stringstream to store the temp command.
					stringstream ss;
					int times = 1;
					// read the input
					time_t start = 0;
					time_t end = 0;
					double time_diff = 0;
					int timeout = 1;

					time(&start);
					bool get_response = true;
					string response;
					while (true) {

						// run the first time.
						if (times > 0) {
							cerr << "Entering first" << endl;
							int n = read(sockfd, buffer, BUFFER_SIZE);
							if (n > 0) {
								buffer[n] = 0;
								ss << buffer;
							}
							times--;
						}

						// using getline() to store the valid command.
						getline(ss, response, '\n');

						// when we do not extract the whole line ('\n)
						// or we are at the end of the previous command line.
						if (ss.eof()) {
							cerr << "Entering second" << endl;
							ss.clear();
							ss << response;

							// read.
							int n = read(sockfd, buffer, BUFFER_SIZE);
							if (n > 0) {
								buffer[n] = 0;
								ss << buffer;
							}
							// set a timeout if we cannot hear from the server within 2 seconds.
							time(&end);
							time_diff = difftime(start, end);
							if(time_diff > timeout) {
								get_response = false;
								break;
							}


							continue;
						} else {
							break;
						}
					}

					if (get_response) {
						// if not valid response
						if (debug_flag) {
							cerr << "The response is: " << response << endl;
						}
						if (response.substr(0, 3) != "250" && response.substr(0, 3) != "220"
								&& response.substr(0, 3) != "354") {
							string msg = "-ERR error response from server for request " +curr + "\r\n";
							do_write(comm_fd, &msg.at(0), msg.size());
							success_sent = false;
							break;
						}
					} else {
						string msg = "-ERR no response\n";
						do_write(comm_fd, &msg.at(0), msg.size());
						success_sent = false;
						break;
					}
				}

				if (success_sent) {
					string msg = "+OK successfully sent\n";
					do_write(comm_fd, &msg.at(0), msg.size());
					if (debug_flag) cout << msg;
					close(sockfd);
				}
			} else {
				string msg = "-ERR no corresponding MX ip\n";
				do_write(comm_fd, &msg.at(0), msg.size());
			}
		} else {
			string msg = "-ERR no corresponding ip address\n";
			do_write(comm_fd, &msg.at(0), msg.size());
		}
	}


	// exit the thread.
	pthread_exit(NULL);
}


// in the server, the user will type in command including the sender email address, receiver email address and the email content.
// in one string: sender#receiver#content.
// we should put these 3 parts after the -v -a options.
int main(int argc, char *argv[])
{

	// first parse the input command using getopt().
	// the flag for -v and -a

	bool aFlag = false;
	int c;
	int port_number = 5100;
	char * pValue = NULL;
	bool pFlag = false;

	while ((c = getopt(argc, argv, "vap:")) != -1) {
		switch(c) {
		case 'v':
			debug_flag = true;
			break;
		case 'a':
			aFlag = true;
			break;
		case 'p':
			pFlag = true;
			pValue = optarg;
			break;
		case '?':
			if (isprint(optopt)) {
				fprintf(stderr, "Unknown option '-%c'.\n", optopt);
				exit(1);
			} else {
				fprintf(stderr, "Unknown option character.\n");
				exit(1);
			}
		default:
			abort();
		}
	}



	// if there is a -a in the command, output and exit.
	if (aFlag) {
		fprintf(stderr, "Name: Jian Li; SEAS login: lijian2.\n");
		exit(1);
	}

	if (pFlag) {
		port_number = atoi(pValue);
	}

	// Then listen the port and connection.
	// create a new socket
	int sockfd = socket(PF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		fprintf(stderr, "Cannot open socket (%s) \n", strerror(errno));
		exit(1);
	}

	// bind()
	struct sockaddr_in servaddr;
	bzero(&servaddr, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = htons(INADDR_ANY);
	servaddr.sin_port = htons(port_number);
	bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));

	// CTRL-C signal handler.
	struct sigaction sigIntHandler;
	sigIntHandler.sa_handler = my_handler;
	sigemptyset(&sigIntHandler.sa_mask);
	sigIntHandler.sa_flags = 0;
	sigaction(SIGINT, &sigIntHandler, NULL);


	// listen to clients.
	listen(sockfd, 10);

	// connect
	while (true) {
		struct sockaddr_in clientaddr;
		socklen_t clientaddrlen = sizeof(clientaddr);

		// get the fd of the client.
		int * comm_fd = new int;
		*comm_fd = accept(sockfd, (struct sockaddr*)&clientaddr, &clientaddrlen);

		//printf("Connection from %s\n", inet_ntoa(clientaddr.sin_addr));
		all_fd_array[*comm_fd] = 1;

		// then show the greeting message.
		string greeting_message = "+OK localhost service ready\r\n";
		do_write(*comm_fd, &greeting_message.at(0), greeting_message.size());


		// creating and running the new thread.
		pthread_t newThread;
		pthread_create(&newThread, NULL, worker_thread, (void*) (comm_fd));

	}


	return 0;
}
