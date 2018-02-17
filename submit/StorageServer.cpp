#include <stdlib.h>
#include <iostream>
#include <cstring>
#include <sstream>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <algorithm>
#include <unordered_map>
#include <vector>
#include <list>

#include "BigTable.h"
#include "Logger.h"

// macros
#define ERRCODE -1
#define MAXCONNECTION 16
#define EMPTY ""
#define MAXLEN 50000000
#define NORMALLEN 1024
#define FOLDER "FOLDER"
#define THRESHOLD 2
#define MEMFILE "mem"
#define BIGTABLEFILE "bigtable"
#define DELETEFILE "deleteInfo"
#define LOGFILE "log"

// enum
enum CMD{
    INVALID, GET, PUT, DEL, MOVE, DIR, LOG, SHUTDOWN
};
enum DEBUGSTATUS{
    OBJECTIVE, D_RECV, D_SEND
};
enum TARGETTYPE{
    MASTER, CLIENT, PRIMARY
};
// structs
struct threadTask{
    int httpFd;
};

// global variables
bool debug;
std::string thisAddr;
std::string masterAddr;
int clientFd;
BigTable* tablet;
Logger* logger;
std::list<int> fdList;
int opCounter;
pthread_mutex_t counterLock;
pthread_mutex_t fdListLock;

// functions
void printError(const char* errorMsg, int err);
void printDebug(std::string msg, DEBUGSTATUS dStatus, TARGETTYPE cType, std::string cAddr);
std::string strTrim(std::string input);
void sendMsg(int fd, std::string msg);
bool recvMsg(int fd, char* buffer, int len);
int getPort(std::string addr);
std::string getIp(std::string addr);
void incrOp();
void clearOp();
void initLogger();
void initLock();
void initBigTable();
void recoverBigTable();
bool checkCmd(std::string input, std::string target);
CMD getCmd(std::string input);
std::vector<std::string> parseCmd(std::string input);
std::string executeCmd(std::string msg);
void recoverLog(std::string logEntries);
void crashHandler(int sig);
void* workerRoutine(void* task);

// function to print error
void printError(const char* errorMsg, int err){
    perror(errorMsg);
    exit(err);
}

// function to print debug info
void printDebug(std::string msg, DEBUGSTATUS dStatus, TARGETTYPE cType, std::string cAddr){
    if(!debug)
        return;
    msg = strTrim(msg);
    cAddr = strTrim(cAddr);
    std::string type;
    switch(cType){
    case CLIENT:
        type = "CLIENT";
        break;
    case PRIMARY:
        type = "PRIMARY";
        break;
    default:
        type = "MASTER";
        break;
    }
    std::cout << "[debug] ";
    switch(dStatus){
    case OBJECTIVE:
        std::cout << msg << " Type: "<< type << " Addr: " << cAddr << std::endl;
        break;
    case D_RECV:
        std::cout << "recv " << msg << " from " << cAddr << "(" << type << ")" << std::endl;
        break;
    case D_SEND:
        std::cout << "send " << msg << " to " << cAddr << "(" << type << ")" << std::endl;
        break;
    }
    std::cout << std::flush;
}

// trim a string
std::string strTrim(std::string input){
    const char* s = input.c_str();
    int len = input.length();
    int startInd = -1;
    int endInd;
    for(int i=0; i<len; i++){
        if(s[i]!=' ' && s[i]!='\n' && s[i]!='\t' && s[i]!='\r'){
            startInd = i;
            break;
        }
    }
    if(startInd == -1)
        return "";
    for(int i=len-1; i>=0; i--){
        if(s[i]!=' ' && s[i]!='\n' && s[i]!='\t' && s[i]!='\r'){
            endInd = i;
            break;
        }
    }
    return input.substr(startInd, endInd+1-startInd);
}

// function to recv message
bool recvMsg(int fd, char* buffer, int len){
	int rcvd = 0;
	while(rcvd < len){
		int n = recv(fd, &buffer[rcvd], len-rcvd, 0);
		if(n < 0){
			printError("Server Recv Failed: ", ERRCODE);
		}
		if(len == 0){
			return false;
		}
		rcvd += n;
	}
	//buffer[len] = '\0';
	return true;
}

// function to send message
void sendMsg(int fd, std::string msg){
	int len = msg.length();
	int sent = 0;
	const char* buffer = msg.c_str();
	while(sent < len){
		int n = send(fd, &buffer[sent], len-sent, 0);
    	if(n < 0){
        	printError("Server Send Failed: ", ERRCODE);
    	}
    	sent += n;
	}
    return;
}

// get port number from url
int getPort(std::string addr){
	int portStartInd;
	int port;
	if((portStartInd = addr.find(":")) == std::string::npos)
		port = 0;
	else if(portStartInd >= addr.length()-1)
		port = 0;
	else{
		std::string portStr = addr.substr(portStartInd+1, addr.length()-portStartInd-1);
		portStr = strTrim(portStr);
		port = atoi(portStr.c_str());
	}
	return port;
}

// get ip address from url
std::string getIp(std::string addr){
	int portStartInd;
	std::string ip;
	if((portStartInd = addr.find(":")) == std::string::npos)
		ip = strTrim(addr);
	else
		ip = strTrim(addr.substr(0, portStartInd));
	return ip;
}

// get file name from address
std::string getFileNameFromAddr(std::string addr){
	int ind;
	std::string addrName = addr;
	if((ind = addrName.find(":")) != std::string::npos)
		addrName.replace(ind, 1, "_");
    while((ind = addrName.find(".")) != std::string::npos){
                addrName.replace(ind,1,"_");
    }
    return addrName;
}

// init empty logger
void initLogger(){
	std::string addrName = getFileNameFromAddr(thisAddr) + "/" + LOGFILE;
	logger = new Logger(addrName);
	logger->clearLog();
}

// init lock
void initLock(){
	fdListLock = PTHREAD_MUTEX_INITIALIZER;
	counterLock = PTHREAD_MUTEX_INITIALIZER;
}

// init empty big table
void initBigTable(){
	std::string addrName = getFileNameFromAddr(thisAddr);
	tablet = new BigTable(addrName);
	return;
}

// init big table from recovery
void recoverBigTable(){
	std::string addrName = getFileNameFromAddr(thisAddr);
	tablet = new BigTable(addrName, MEMFILE, BIGTABLEFILE, DELETEFILE);
	return;
}

// increase operation counter
void incrOp(){
	pthread_mutex_lock(&counterLock);
	opCounter ++;
	pthread_mutex_unlock(&counterLock);
}

// clear operation counter
void clearOp(){
	pthread_mutex_lock(&counterLock);
	opCounter = 0;
	pthread_mutex_unlock(&counterLock);
}

// check pass msg
bool checkCmd(std::string input, std::string target){
    if(input.length() < target.length())
        return false;
    //std::transform(input.begin(), input.end(), input.begin(), ::tolower);
    int flag = strncmp(input.c_str(), target.c_str(), target.length());
    if(flag == 0)
        return true;
    return false;
}

// get cmd type
CMD getCmd(std::string input){
    if(checkCmd(input, "GET "))
        return GET;
    if(checkCmd(input, "PUT "))
        return PUT;
    if(checkCmd(input, "DEL "))
        return DEL;
    if(checkCmd(input, "MOVE "))
        return MOVE;
    if(checkCmd(input, "DIR "))
        return DIR;
    if(checkCmd(input, "LOG"))
    	return LOG;
    if(checkCmd(input, "SHUTDOWN"))
    	return SHUTDOWN;
    return INVALID;
}

// pass cmd from other servers
std::vector<std::string> parseCmd(std::string input){
	//std::transform(input.begin(), input.end(), input.begin(), ::tolower);
	std::vector<std::string> argList;
	int firstArgInd = input.find(" ");
	if(firstArgInd == std::string::npos){
		argList.push_back(EMPTY);
		return argList;
	}
	input = input.substr(firstArgInd+1, input.length()-firstArgInd-1);
	//input = strTrim(input);
	int sharpInd;
	while((sharpInd = input.find("#")) != std::string::npos){
		argList.push_back(input.substr(0, sharpInd));
		input = input.substr(sharpInd+1, input.length()-sharpInd-1);
	}
	argList.push_back(input);
	return argList;
}

std::string executeCmd(std::string msg){
	char* recvBuffer = new char[NORMALLEN];
	int len;
	// check and parse the cmd
	std::string logEntry = msg;//TODO: BETTER DELIMITER IS NEEDED
	bool writeLog = false;
	bool canIncrOp = false;
	CMD cmd = getCmd(msg);
	std::vector<std::string> argVec = parseCmd(msg);
	std::ostringstream keyBuilder;
	std::ostringstream msgBuilder;
	std::string userName = argVec[0], fileAddr, oldFileAddr, fileType, fileContent, colKey, oldColKey, reply, forward, logContent;
	int fileSize, i;
	bool flag, shutDown = false;

	switch(cmd){
	case GET:
		fileAddr = argVec[1];
		fileType = argVec[2];
		keyBuilder << fileAddr << "#" << fileType;
		colKey = keyBuilder.str();
		fileContent = tablet->get_file(userName, colKey);
		//cout << "get size: " << fileContent.length() << std::endl;
		if(fileContent.compare(EMPTY) != 0){
			msgBuilder << "+OK " << fileContent;
			reply = msgBuilder.str();
			msgBuilder.clear();
			msgBuilder.str("");
			msgBuilder << "FINISH " << thisAddr;
		    forward = msgBuilder.str();
			if((len = send(clientFd, forward.c_str(), forward.length(),0)) < 0)
				printError("Send to master failed: ", ERRCODE);
			printDebug(forward, D_SEND, MASTER, masterAddr);
			if((len = recv(clientFd, recvBuffer, NORMALLEN, 0)) < 0)
				printError("Recv from master failed: ", ERRCODE);
			recvBuffer[len] = '\0';
			printDebug(recvBuffer, D_RECV, MASTER, masterAddr);
		}
		else
			reply = "-ERR";
		break;
	case PUT:
		fileAddr = argVec[1];
		fileType = argVec[2];
		fileContent = "";
		i = 3;
		while(i < argVec.size()-1){
			fileContent += argVec[i];
			fileContent += "#";
			i ++;
		}
		fileContent += argVec[argVec.size()-1];
		//cout << "put size: " << fileContent.length() << std::endl;
		keyBuilder << fileAddr << "#" << fileType;
		colKey = keyBuilder.str();

		if(tablet->put_file(userName, colKey, fileContent.c_str(), fileContent.length(),fileType)){
			reply = "+OK";
			msgBuilder << "SPUT " << userName << "#" << colKey << "#" << thisAddr;
			forward = msgBuilder.str();
			if((len = send(clientFd, forward.c_str(), forward.length(),0)) < 0)
				printError("Send to master failed: ", ERRCODE);
			printDebug(forward, D_SEND, MASTER, masterAddr);
			if((len = recv(clientFd, recvBuffer, NORMALLEN, 0)) < 0)
				printError("Recv from master failed: ", ERRCODE);
			recvBuffer[len] = '\0';
			printDebug(recvBuffer, D_RECV, MASTER, masterAddr);
			writeLog = true;
			canIncrOp = true;
		}
		else
			reply = "-ERR";
		break;
	case DIR:
		fileAddr = argVec[1];
		fileType = FOLDER;
		keyBuilder << fileAddr << "#" << fileType;
		colKey = keyBuilder.str();
		if(tablet->put_folder(userName, colKey, fileType)){
			reply = "+OK";
			msgBuilder << "SDIR " << userName << "#" << colKey << "#" << thisAddr;
			forward = msgBuilder.str();
			if((len = send(clientFd, forward.c_str(), forward.length(),0)) < 0)
				printError("Send to master failed: ", ERRCODE);
			printDebug(forward, D_SEND, MASTER, masterAddr);
			if((len = recv(clientFd, recvBuffer, NORMALLEN, 0)) < 0)
				printError("Recv from master failed: ", ERRCODE);
			recvBuffer[len] = '\0';
			printDebug(recvBuffer, D_RECV, MASTER, masterAddr);
			writeLog = true;
			canIncrOp = true;
		}
		else
			reply = "-ERR";
		break;
	case DEL:
		fileAddr = argVec[1];
		fileType = argVec[2];
		keyBuilder << fileAddr << "#" << fileType;
		colKey = keyBuilder.str();
		if(fileType.compare(FOLDER) == 0){
			flag = tablet->delete_folder(userName, colKey, fileType);
		}
		else{
			flag = tablet->delete_file(userName, colKey);
		}
		if(flag){
			reply = "+OK";
			msgBuilder << "SDEL " << userName << "#" << colKey << "#" << thisAddr;
			forward = msgBuilder.str();
			if((len = send(clientFd, forward.c_str(), forward.length(),0)) < 0)
				printError("Send to master failed: ", ERRCODE);
			printDebug(forward, D_SEND, MASTER, masterAddr);
			if((len = recv(clientFd, recvBuffer, NORMALLEN, 0)) < 0)
				printError("Recv from master failed: ", ERRCODE);
			recvBuffer[len] = '\0';
			printDebug(recvBuffer, D_RECV, MASTER, masterAddr);
			writeLog = true;
			canIncrOp = true;
		}
		else
			reply = "-ERR";
		break;
	case MOVE:
		std::cout << "f1\n";
		oldFileAddr = argVec[1];
		fileAddr = argVec[2];
		fileType = argVec[3];
		std::cout << "f2\n";
		keyBuilder << oldFileAddr << "#" << fileType;
		oldColKey = keyBuilder.str();
		keyBuilder.clear();
		keyBuilder.str("");
		keyBuilder << fileAddr << "#" << fileType;
		colKey = keyBuilder.str();
		flag = tablet->rename_file(userName, oldColKey, colKey);
		if(flag){
			reply = "+OK";
			msgBuilder << "SMOVE " << userName << "#" << oldFileAddr << "#" << colKey << "#" << thisAddr;
			forward = msgBuilder.str();
			if((len = send(clientFd, forward.c_str(), forward.length(),0)) < 0)
				printError("Send to master failed: ", ERRCODE);
			printDebug(forward, D_SEND, MASTER, masterAddr);
			if((len = recv(clientFd, recvBuffer, NORMALLEN, 0)) < 0)
				printError("Recv from master failed: ", ERRCODE);
			recvBuffer[len] = '\0';
			printDebug(recvBuffer, D_RECV, MASTER, masterAddr);
			writeLog = true;
			canIncrOp = true;
		}
		else
			reply = "-ERR";
		// if(fileType.compare(FOLDER) == 0){
		// 	std::cout << "f3\n";
		// 	keyBuilder << fileAddr << "#" << fileType;
		// 	colKey = keyBuilder.str();
		//         flag = tablet->move_folder(userName, oldColKey, colKey, fileType);
		// 	if(flag){
		// 		reply = "+OK";
		// 		msgBuilder << "SMOVE " << userName << "#" << oldFileAddr << "#" << colKey << "#" << thisAddr;
		// 		forward = msgBuilder.str();
		// 		if((len = send(clientFd, forward.c_str(), forward.length(),0)) < 0)
		// 			printError("Send to master failed: ", ERRCODE);
		// 		printDebug(forward, D_SEND, MASTER, masterAddr);
		// 		if((len = recv(clientFd, recvBuffer, NORMALLEN, 0)) < 0)
		// 			printError("Recv from master failed: ", ERRCODE);
		// 		recvBuffer[len] = '\0';
		// 		printDebug(recvBuffer, D_RECV, MASTER, masterAddr);
		// 		writeLog = true;
		// 		canIncrOp = true;
		// 	}
		// 	else
		// 		reply = "-ERR";
		// }
		// else{
		// 	std::cout << "f4\n";
		// 	keyBuilder << fileAddr << "#" << fileType;
		// 	colKey = keyBuilder.str();
		// 	flag = tablet->move_file(userName, oldColKey, colKey, fileType);
		// 	std::cout << "f5\n";
		// 	if(flag){
		// 		reply = "+OK";
		// 		msgBuilder << "SMOVE " << userName << "#" << oldFileAddr << "#" << colKey << "#" << thisAddr;
		// 		forward = msgBuilder.str();
		// 		if((len = send(clientFd, forward.c_str(), forward.length(),0)) < 0)
		// 			printError("Send to master failed: ", ERRCODE);
		// 		printDebug(forward, D_SEND, MASTER, masterAddr);
		// 		if((len = recv(clientFd, recvBuffer, NORMALLEN, 0)) < 0)
		// 			printError("Recv from master failed: ", ERRCODE);
		// 		recvBuffer[len] = '\0';
		// 		printDebug(recvBuffer, D_RECV, MASTER, masterAddr);
		// 		writeLog = true;
		// 		canIncrOp = true;
		// 	}
		// 	else
		// 		reply = "-ERR";
		// }
		break;
	case LOG:
		logger->getLog(logContent);
		reply = logContent;
		break;
	case SHUTDOWN:
		reply = "+OK";
		shutDown = true;
		break;
	default:
		reply = "-ERR Invalid Command";
		break;
	}
    //tablet->print_bigtable();

	if(writeLog)
		logger->appendLog(logEntry);
	if(canIncrOp)
		incrOp();
	if(opCounter >= THRESHOLD){
		if(!tablet->flush_tables(MEMFILE, BIGTABLEFILE, DELETEFILE))
			printError("checkpoint failed: ", ERRCODE);
		clearOp();
		logger->clearLog();
	}
	delete recvBuffer;
	if(shutDown){
		printDebug("SHUTTING DOWN!!!", OBJECTIVE, CLIENT, EMPTY);
		crashHandler(SIGINT);
	}
	return reply;
}

void recoverLog(std::string logEntries){
	int ind;
	std::string remainEntries = logEntries;
	while((ind = remainEntries.find(",")) != remainEntries.length()-1){
		if(ind == std::string::npos)
			break;
		std::string entry = remainEntries.substr(0, ind);
		remainEntries = remainEntries.substr(ind+1, remainEntries.length()-ind-1);
		executeCmd(entry);
	}
	if(ind != std::string::npos){
		std::string entry = remainEntries.substr(0, ind);
		executeCmd(entry);
	}
	return;
}

// sig handler for ctrl + c(SIGINT)
void crashHandler(int sig){
	if(sig == SIGINT){
		pthread_mutex_lock(&fdListLock);
		while(!fdList.empty()){
			close(fdList.front());
			fdList.pop_front();
		}
		pthread_mutex_unlock(&fdListLock);
		close(clientFd);
		exit(ERRCODE);
	}
}

// thread routine for http
void* workerRoutine(void* task){
	printDebug("NEW CONNECTION", OBJECTIVE, CLIENT, EMPTY);
	threadTask* tTask = (threadTask *)task;
	int httpFd = tTask->httpFd;
	pthread_mutex_lock(&fdListLock);
	fdList.push_back(httpFd);
	pthread_mutex_unlock(&fdListLock);
	char* recvBuffer = new char[MAXLEN];
	int len;

	while(1){
		// receive msg length
		if((len = recv(httpFd, recvBuffer, MAXLEN, 0)) < 0)
			printError("Server Recv Length Failed: ", ERRCODE);
		if(len == 0){
			delete recvBuffer;
			printDebug("DISCONNECTED", OBJECTIVE, CLIENT, EMPTY);
			close(httpFd);
			pthread_mutex_lock(&fdListLock);
			fdList.remove(httpFd);
			pthread_mutex_unlock(&fdListLock);
			pthread_exit(NULL);
		}
		recvBuffer[len] = '\0';
		printDebug(recvBuffer, D_RECV, CLIENT, EMPTY);
		//std::cout << recvBuffer << std::endl;
		int recvLen = atoi(recvBuffer);
		std::string ack = "+OK";
		sendMsg(httpFd, ack.c_str());
		printDebug(ack, D_SEND, CLIENT, EMPTY);
		// receive msg
		if(!recvMsg(httpFd, recvBuffer, recvLen)){
			delete recvBuffer;
			printDebug("DISCONNECTED", OBJECTIVE, CLIENT, EMPTY);
			close(httpFd);
			pthread_mutex_lock(&fdListLock);
			fdList.remove(httpFd);
			pthread_mutex_unlock(&fdListLock);
			pthread_exit(NULL);
		}
		recvBuffer[recvLen] = '\0';
		printDebug(recvBuffer, D_RECV, CLIENT, EMPTY);
		//std::cout << recvBuffer << std::endl;

		std::string reply = executeCmd(recvBuffer);
		std::string replySize = to_string(reply.length());
		sendMsg(httpFd, replySize);
		printDebug(replySize, D_SEND, CLIENT, EMPTY);
		len = recv(httpFd, recvBuffer, MAXLEN, 0); //TODO: error handler
		if(len < 0)
			printError("Recv Failed: ", ERRCODE);
		if(len == 0){
			delete recvBuffer;
			printDebug("DISCONNECTED", OBJECTIVE, CLIENT, EMPTY);
			close(httpFd);
			pthread_mutex_lock(&fdListLock);
			fdList.remove(httpFd);
			pthread_mutex_unlock(&fdListLock);
			pthread_exit(NULL);
		}
		recvBuffer[len] = '\0';
		printDebug(recvBuffer, D_RECV, CLIENT, EMPTY);
		sendMsg(httpFd, reply);
		printDebug(reply, D_SEND, CLIENT, EMPTY);
	}
    pthread_exit(NULL);
	return NULL;
}

int main(int argc, char *argv[]){
	// parse command
    int opt;
    masterAddr = EMPTY;
    thisAddr = EMPTY;
    bool mFlag = false;
    bool vFlag = false;
    bool aFlag = false;
    while ((opt = getopt (argc, argv, "m:a:v")) != -1){
        switch(opt){
        case 'm':
            mFlag = true;
            masterAddr = optarg;
            break;
        case 'a':
            aFlag = true;
            thisAddr = optarg;
            break;
        case 'v':
            vFlag = true;
            break;
        default:
            printError("Usage: ./StorageServer -m masterAddress -a thisAddress [-v] \n", ERRCODE);
            break;
        }
    }
    debug = vFlag;
    if(masterAddr.compare(EMPTY) == 0 || thisAddr.compare(EMPTY) == 0 || argc-optind != 0){
    	printError("Usage: ./StorageServer -m masterPort -a thisAddress [-v] \n", ERRCODE);
    }
    opCounter = 0;
    initLogger();
    initLock();

    signal(SIGINT, crashHandler);

    // variables for socket
    int serverFd, httpFd, recoverFd;
    struct sockaddr_in serverAddr, clientAddr, recoverAddr;
    unsigned int sockaddrSize = sizeof(struct sockaddr);
    struct sockaddr_in httpAddr;
    unsigned int httpAddrSize = sizeof(httpAddr);//initialize to prevent Accept:Invalid Argument

    // variables for worker thread
    pthread_attr_t attr;

    // initialize server socket and related variables
    if((serverFd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        printError("Socket Init Failed: ", ERRCODE);
    serverAddr.sin_family=AF_INET;
    serverAddr.sin_port=htons(getPort(thisAddr));
    serverAddr.sin_addr.s_addr=INADDR_ANY;
    bzero(&(serverAddr.sin_zero),8);

    // initialize client socket and related variables
    if((clientFd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        printError("Socket Init Failed: ", ERRCODE);
    clientAddr.sin_family=AF_INET;
    clientAddr.sin_port=htons(getPort(masterAddr));
    clientAddr.sin_addr.s_addr=inet_addr(getIp(masterAddr).c_str());
    //TODO: outer IP
    //clientAddr.sin_addr.s_addr=inet_addr("127.0.0.1");
    bzero(&(clientAddr.sin_zero),8);

    // bind
    if(bind(serverFd,(struct sockaddr *)&serverAddr, sockaddrSize) < 0)
        printError("Bind Failed: ", ERRCODE);

    // listen
    if(listen(serverFd, MAXCONNECTION) < 0)
        printError("Listen Failed: ", ERRCODE);

    // connect
    if(connect(clientFd, (struct sockaddr *)&clientAddr, sockaddrSize) < 0){
    	printError("Connect Failed: ", ERRCODE);
    }

    int len;
    char* recvBuffer = new char[MAXLEN];
    sendMsg(clientFd, thisAddr);
    printDebug(thisAddr, D_SEND, MASTER, masterAddr);
    if((len = recv(clientFd, recvBuffer, MAXLEN, 0)) < 0)
        printError("Recv Failed: ", ERRCODE);
    recvBuffer[len] = '\0';
    std::string msg = recvBuffer;
    printDebug(msg, D_RECV, MASTER, masterAddr);
    if(msg.compare("+OK") != 0){
    	printDebug("RECOVER FROM CHECKPOINT", OBJECTIVE, PRIMARY, thisAddr);
    	// READ BIGTABLE AND MEMDATA FIRST
    	recoverBigTable();

    	if((recoverFd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        	printError("Socket Init Failed: ", ERRCODE);
    	recoverAddr.sin_family=AF_INET;
   		recoverAddr.sin_port=htons(getPort(msg));
    	recoverAddr.sin_addr.s_addr=inet_addr(getIp(msg).c_str());
    	//clientAddr.sin_addr.s_addr=inet_addr("127.0.0.1");
    	bzero(&(recoverAddr.sin_zero),8);
    	printDebug("RECOVER FROM LOG", OBJECTIVE, PRIMARY, thisAddr);
    	// TODO: RECOVER FROM LOG
    	if(connect(recoverFd, (struct sockaddr *)&recoverAddr, sockaddrSize) < 0){
    		printError("Connect Failed: ", ERRCODE);
    	}
    	std::string logRequest = "LOG";
    	std::string reqLen = to_string(logRequest.length());
    	std::string ack = "+OK";
    	sendMsg(recoverFd, reqLen);
    	printDebug(reqLen, D_SEND, PRIMARY, msg);
    	len = recv(recoverFd, recvBuffer, MAXLEN, 0);
    	recvBuffer[len] = '\0';
    	printDebug(recvBuffer, D_RECV, PRIMARY, msg);
    	sendMsg(recoverFd, logRequest);
    	printDebug(logRequest, D_SEND, PRIMARY, msg);
    	len = recv(recoverFd, recvBuffer, MAXLEN, 0);
    	recvBuffer[len] = '\0';
    	printDebug(recvBuffer, D_RECV, PRIMARY, msg);
    	int recvLen = atoi(recvBuffer);
    	sendMsg(recoverFd, ack);
    	printDebug(ack, D_SEND, PRIMARY, msg);
    	recvMsg(recoverFd, recvBuffer, recvLen);
    	recvBuffer[recvLen] = '\0';
    	printDebug(recvBuffer, D_RECV, PRIMARY, msg);
    	close(recoverFd);
    	recoverLog(recvBuffer);
    }
    else
    	initBigTable();

    delete recvBuffer;
    cout << "********BIG TABLE********\n";
    tablet->print_bigtable();
    cout << "*************************\n";

    // accept connection and create thread to handle request
    pthread_attr_init(&attr);
    while(1){
        pthread_t thread;
        if((httpFd = accept(serverFd, (struct sockaddr *)&httpAddr, &httpAddrSize)) < 0){
            printError("Accept Failed: ", ERRCODE);
        }
        struct threadTask task;
        task.httpFd = httpFd;
        pthread_create(&thread, &attr, workerRoutine, &task);
    }
}
