#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <cstring>
#include <ctime>
#include <sstream>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <errno.h>
#include <signal.h>
#include <algorithm>
#include <limits>
#include <utility>
#include <vector>
#include <unordered_map>
#include <list>

// macros
#define ERRCODE -1
#define MAXCONNECTION 16
#define EMPTY ""
#define MAXLEN 65536
#define MAXIDLETIME 10

// enum
enum CMD{
    INVALID, REG, LOGIN, LOGOUT, PASS, STATE, TABLE, GET, PUT, SEND, DEL, MOVE, DIR, VIEW, SPUT, SDEL, SMOVE, SDIR, FINISH
};
enum DEBUGSTATUS{
    OBJECTIVE, D_RECV, D_SEND
};
enum WORKINGSTATUS{
    UNCONNECTED, WORKING, CRASH
};
enum CLIENTTYPE{
    UNKNOWN, FRONTEND, BACKEND_PRIMARY, BACKEND_REPLICA
};
enum EMAILTYPE{
    OTHER, LOCAL, REMOTE
};

// structs
struct threadTask{
    int clientFd;
};
struct userStatus{
    bool online;
    std::time_t lastActiveTime;
};
struct serverStatus{
    CLIENTTYPE type;
    WORKINGSTATUS wStatus;
    int load;
};

// global variables
bool debug;
std::list<std::string> backendList;
std::unordered_map<std::string, std::string> passwordMap;
std::unordered_map<std::string, struct userStatus> onlineMap;
std::unordered_map<std::string, struct serverStatus> serverLoadMap; 
std::unordered_map<std::string, std::string> replicaMap;
std::unordered_map<std::string, std::unordered_map<std::string, std::pair<std::string, std::string>>> metadataMap;
pthread_mutex_t passwordMapLock;
pthread_mutex_t onlineMapLock;
pthread_mutex_t serverLoadMapLock;
pthread_mutex_t replicaMapLock;
pthread_mutex_t metadataMapLock;
std::string smtpClientAddr;

// functions
void alarmHandler(int sig);
void printError(const char* errorMsg, int err);
void printDebug(std::string msg, DEBUGSTATUS dStatus, CLIENTTYPE cType, std::string cAddr);
std::time_t getCurrTime();
void updateTime(std::string userName);
void incrLoad(std::string serverAddr);
void decrLoad(std::string serverAddr);
void sendMsg(int fd, std::string msg);
std::string strTrim(std::string input);
void initFromConfig(std::string configPath);
void initTimer();
void initLock();
void kickIdleUsers();
void handleCrashServer(std::string serverAddr, CLIENTTYPE type);
std::string findAvailableServer();
bool checkCmd(std::string input, std::string target);
std::vector<std::string> parseCmd(std::string input);
void initClientType(std::string clientAddr);
CLIENTTYPE getClientType(std::string clientAddr);
std::string getPrimaryAddr(std::string rowKey, std::string colKey);
bool getFullAddr(std::string rowKey, std::string colKey, std::string& fullAddr);
bool findUserName(std::string userName);
bool findRowKey(std::string rowKey);
bool findColKey(std::string rowKey, std::string colKey);
void removeFile(std::string rowKey, std::string colKey);
void addFile(std::string rowKey, std::string colKey, std::string value);
std::string buildView(std::string userName, std::string fileType);
EMAILTYPE getEmailType(std::string receiverAddr);
void* workerRoutine(void* task);

// SIGALRM handler
void alarmHandler(int sig){
    if(sig == SIGALRM){
        kickIdleUsers();
        signal(SIGALRM, alarmHandler);
        alarm(MAXIDLETIME);
        return;
    }
    return;
}

// function to print error
void printError(const char* errorMsg, int err){
    perror(errorMsg);
    exit(err);
}

// function to print debug info
void printDebug(std::string msg, DEBUGSTATUS dStatus, CLIENTTYPE cType, std::string cAddr){
    if(!debug)
        return;
    msg = strTrim(msg);
    cAddr = strTrim(cAddr);
    std::string type;
    switch(cType){
    case BACKEND_PRIMARY:
        type = "BACKEND_PRIMARY";
        break;
    case BACKEND_REPLICA:
        type = "BACKEND_REPLICA";
        break;
    case FRONTEND:
        type = "FRONTEND";
        break;
    default:
        type = "UNKNOWN";
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

// get current time stamp
std::time_t getCurrTime(){
    return std::time(0);
}

// update user's last active time
void updateTime(std::string userName){
    pthread_mutex_lock(&onlineMapLock);
    onlineMap[userName].lastActiveTime = getCurrTime();
    pthread_mutex_unlock(&onlineMapLock);
    return;
}

// increase load of given server
void incrLoad(std::string serverAddr){
    int ind;
    pthread_mutex_lock(&serverLoadMapLock);
    if((ind = serverAddr.find("#")) == std::string::npos)
        serverLoadMap[serverAddr].load = serverLoadMap[serverAddr].load + 1;
    else{
        std::string addr1 = serverAddr.substr(0, ind);
        std::string addr2 = serverAddr.substr(ind+1, serverAddr.length()-ind-1);
        serverLoadMap[addr1].load = serverLoadMap[addr1].load + 1;
        serverLoadMap[addr2].load = serverLoadMap[addr2].load + 1;
    }
    pthread_mutex_unlock(&serverLoadMapLock);
}

// decrease load of given server
void decrLoad(std::string serverAddr){
    pthread_mutex_lock(&serverLoadMapLock);
    if(serverLoadMap[serverAddr].load > 0)
        serverLoadMap[serverAddr].load = serverLoadMap[serverAddr].load - 1;
    pthread_mutex_unlock(&serverLoadMapLock);
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

// init from config file
void initFromConfig(std::string configPath){
    std::ifstream infile(configPath);
    std::string line;
    while(getline(infile, line)){
        if(line.compare(EMPTY) == 0)
            break;
        int ind;
        if((ind = line.find(",")) == std::string::npos){
            smtpClientAddr = strTrim(line);
        }
        else{
            std::string storageServer = strTrim(line.substr(0, ind));
            std::string replicaServer = strTrim(line.substr(ind+1, line.length()-ind-1));
            struct serverStatus primaryStatus, replicaStatus;
            primaryStatus.load = 0;
            primaryStatus.wStatus = UNCONNECTED;
            primaryStatus.type = BACKEND_PRIMARY;
            replicaStatus.load = 0;
            replicaStatus.wStatus = UNCONNECTED;
            replicaStatus.type = BACKEND_REPLICA;
            pthread_mutex_lock(&serverLoadMapLock);
            serverLoadMap[storageServer] = primaryStatus;
            serverLoadMap[replicaServer] = replicaStatus;
            pthread_mutex_unlock(&serverLoadMapLock);
            pthread_mutex_lock(&replicaMapLock);
            replicaMap[storageServer] = replicaServer;
            replicaMap[replicaServer] = storageServer;
            pthread_mutex_unlock(&replicaMapLock);
            backendList.push_back(storageServer);
            backendList.push_back(replicaServer);
        }
    }
    //////HARD CODE FOR TEST
    // std::unordered_map<std::string, std::string> m;
    // m["e1#EMAIL"] = "127.0.0.1:7000";
    // m["e2#EMAIL"] = "127.0.0.1:7001";
    // m["e3#EMAIL"] = "127.0.0.1:7000";
    // m["d1#EMAIL"] = "127.0.0.1:7001";
    // m["d2#EMAIL"] = "127.0.0.1:7001";
    // m["d3#EMAIL"] = "127.0.0.1:7000";
    // m["f1#FOLDER"] = "127.0.0.1:7000";
    // m["f2#FOLDER"] = "127.0.0.1:7001";
    // m["f3#FOLDER"] = "127.0.0.1:7000";
    // m["f4/f5/d4#DRIVE"] = "127.0.0.1:7001";
    // metadataMap["test"] = m;
    passwordMap["yang"] = "aaa";
    passwordMap["jian"] = "aaa";
    passwordMap["mao"] = "aaa";
    passwordMap["chen"] = "aaa";
    struct userStatus status;
    status.online = false;
    status.lastActiveTime = 0;
    onlineMap["yang"] = status;
    struct userStatus status2;
    status2.online = false;
    status2.lastActiveTime = 0;
    onlineMap["jian"] = status;
    struct userStatus status3;
    status3.online = false;
    status3.lastActiveTime = 0;
    onlineMap["mao"] = status;
    struct userStatus status4;
    status4.online = false;
    status4.lastActiveTime = 0;
    onlineMap["chen"] = status;
    std::pair<std::string, std::string> addrPair;
    std::get<0>(addrPair) = "EMPTY";
    std::get<1>(addrPair) = EMPTY;
    std::unordered_map<std::string, std::pair<std::string, std::string>> m;
    m["EMPTY#EMPTY"] = addrPair;
    metadataMap["EMPTY"] = m;
    //////
    return;
}
    
// init Timer for idle user check
void initTimer(){
    signal(SIGALRM, alarmHandler);
    alarm(MAXIDLETIME);
    return;
}

// init Lock for all tables
void initLock(){
    passwordMapLock = PTHREAD_MUTEX_INITIALIZER;
    onlineMapLock = PTHREAD_MUTEX_INITIALIZER;
    serverLoadMapLock = PTHREAD_MUTEX_INITIALIZER;
    metadataMapLock = PTHREAD_MUTEX_INITIALIZER;
    replicaMapLock = PTHREAD_MUTEX_INITIALIZER;
}

// kick out idle users
void kickIdleUsers(){
    // TODO: might need trylock
    std::unordered_map<std::string, struct userStatus>::iterator it;
    for(it = onlineMap.begin(); it != onlineMap.end(); it ++){
        if(!(it->second).online)
            continue;
        std::time_t idleDuration = getCurrTime() - (it->second).lastActiveTime;
        if(idleDuration > MAXIDLETIME)
            onlineMap[it->first].online = false;
    }
    return;
}

// update global server info when some server is down
void handleCrashServer(std::string serverAddr, CLIENTTYPE cType){
    pthread_mutex_lock(&serverLoadMapLock);
    if(serverLoadMap[serverAddr].wStatus == CRASH || serverLoadMap[serverAddr].type == UNKNOWN){
        pthread_mutex_unlock(&serverLoadMapLock);
        return;
    }
    if(serverLoadMap[serverAddr].type == FRONTEND){
        serverLoadMap.erase(serverLoadMap.find(serverAddr));
        pthread_mutex_unlock(&serverLoadMapLock);
        return;
    }
    pthread_mutex_unlock(&serverLoadMapLock);
    std::string primaryAddr;
    std::string replicaAddr;
    printDebug("SERVER CRASHES!!!", OBJECTIVE, cType, serverAddr);
    switch(cType){
    case BACKEND_PRIMARY:{
        primaryAddr = serverAddr;
        pthread_mutex_lock(&replicaMapLock);
        replicaAddr = replicaMap[primaryAddr];
        pthread_mutex_unlock(&replicaMapLock);
        pthread_mutex_lock(&serverLoadMapLock);
        serverLoadMap[primaryAddr].load = 0;
        serverLoadMap[primaryAddr].wStatus = CRASH;
        serverLoadMap[primaryAddr].type = BACKEND_REPLICA;
        serverLoadMap[replicaAddr].type = BACKEND_PRIMARY;
        pthread_mutex_unlock(&serverLoadMapLock);
    }
        break;
    case BACKEND_REPLICA:{
        pthread_mutex_lock(&serverLoadMapLock);
        serverLoadMap[serverAddr].load = 0;
        serverLoadMap[serverAddr].wStatus = CRASH;
        pthread_mutex_unlock(&serverLoadMapLock);
    }
        break;
    default:
        break; 
    }
    return;
}

// find server with lowest load
std::string findAvailableServer(){
    pthread_mutex_lock(&serverLoadMapLock);
    std::unordered_map<std::string, struct serverStatus>::iterator it;
    int minLoad = std::numeric_limits<int>::max();
    std::string minAddr = EMPTY;
    std::ostringstream addrBuilder, debugBuilder;
    for(it = serverLoadMap.begin(); it != serverLoadMap.end(); it ++){
        //std::cout<<"server load:"<<it->first<<"  "<<(it->second).load<<std::endl;
        if((it->second).wStatus == WORKING && (it->second).type == BACKEND_PRIMARY && (it->second).load <= minLoad){
            minAddr = it->first;
            minLoad = (it->second).load;
        }
    }
    pthread_mutex_unlock(&serverLoadMapLock);
    if(minAddr.compare(EMPTY) == 0){
        printDebug("Find no available server", OBJECTIVE, UNKNOWN, EMPTY);
        return EMPTY;
    }
    addrBuilder << minAddr;
    pthread_mutex_lock(&replicaMapLock);
    //std::unordered_map<std::string, std::string>::iterator itr = replicaMap.find(minAddr);
    std::string replicaAddr = replicaMap[minAddr];
    pthread_mutex_unlock(&replicaMapLock);
    pthread_mutex_lock(&serverLoadMapLock);
    if(serverLoadMap[replicaAddr].wStatus == WORKING)
        addrBuilder << "#" << replicaAddr;
    pthread_mutex_unlock(&serverLoadMapLock);
    debugBuilder << "Find available server " << minAddr << " with load " << minLoad << std::endl;
    printDebug(debugBuilder.str(), OBJECTIVE, UNKNOWN, EMPTY);
    return addrBuilder.str();
}

// check cmd from other servers
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
    if(checkCmd(input, "REG "))
        return REG;
    if(checkCmd(input, "LOGIN "))
        return LOGIN;
    if(checkCmd(input, "LOGOUT "))
        return LOGOUT;
    if(checkCmd(input, "PASS "))
        return PASS;
    if(checkCmd(input, "STATE"))
        return STATE;
    if(checkCmd(input, "TABLE"))
        return TABLE;
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
    if(checkCmd(input, "VIEW "))
        return VIEW;
    if(checkCmd(input, "SEND "))
        return SEND;
    if(checkCmd(input, "SPUT "))
        return SPUT;
    if(checkCmd(input, "SDEL "))
        return SDEL;
    if(checkCmd(input, "SMOVE "))
        return SMOVE;
    if(checkCmd(input, "SDIR "))
        return SDIR;
    if(checkCmd(input, "FINISH "))
        return FINISH;
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
    input = strTrim(input);
    int sharpInd;
    while((sharpInd = input.find("#")) != std::string::npos){
        argList.push_back(input.substr(0, sharpInd));
        input = input.substr(sharpInd+1, input.length()-sharpInd-1);
    }
    argList.push_back(input);
    return argList;
}

// init client type from address
void initClientType(std::string clientAddr){
    std::string addr = strTrim(clientAddr);
    // std::unordered_map<std::string, std::string>::iterator it;
    // pthread_mutex_lock(&replicaMapLock);
    // for(it = replicaMap.begin(); it != replicaMap.end(); it++){
    //     if(addr.compare(it->first) == 0){
    //         pthread_mutex_lock(&serverLoadMapLock);
    //         serverLoadMap[addr].type = BACKEND_PRIMARY;
    //         serverLoadMap[addr].wStatus = WORKING;
    //         serverLoadMap[addr].load = 0;
    //         pthread_mutex_unlock(&serverLoadMapLock);
    //         pthread_mutex_unlock(&replicaMapLock);
    //         return;
    //     }
    //     if(addr.compare(it->second) == 0){
    //         pthread_mutex_lock(&serverLoadMapLock);
    //         serverLoadMap[addr].type = BACKEND_REPLICA;
    //         serverLoadMap[addr].wStatus = WORKING;
    //         serverLoadMap[addr].load = 0;
    //         pthread_mutex_unlock(&serverLoadMapLock);
    //         pthread_mutex_unlock(&replicaMapLock);
    //         return;
    //     }
    // }
    // pthread_mutex_unlock(&replicaMapLock);
    pthread_mutex_lock(&serverLoadMapLock);
    if(serverLoadMap.find(addr) != serverLoadMap.end()){
        serverLoadMap[addr].wStatus = WORKING;
        serverLoadMap[addr].load = 0;
    }
    else{
        struct serverStatus newStatus;
        newStatus.type = FRONTEND;
        newStatus.wStatus = WORKING;
        newStatus.load = std::numeric_limits<int>::max();
        serverLoadMap[addr] = newStatus;
    }
    pthread_mutex_unlock(&serverLoadMapLock);
    return;
}

// get clientType from addr
CLIENTTYPE getClientType(std::string clientAddr){
    std::string addr = strTrim(clientAddr);
    std::unordered_map<std::string, std::string>::iterator it;
    pthread_mutex_lock(&serverLoadMapLock);
    if(serverLoadMap.find(addr) == serverLoadMap.end()){
        pthread_mutex_unlock(&serverLoadMapLock);
        return FRONTEND;//UNKNOWN???
    }
    CLIENTTYPE cType = serverLoadMap[addr].type;
    pthread_mutex_unlock(&serverLoadMapLock);
    return cType;
}

// get primary Addr in two candidates
std::string getPrimaryAddr(std::string rowKey, std::string colKey){
    pthread_mutex_lock(&metadataMapLock);
    std::pair<std::string, std::string> addrPair = metadataMap[rowKey][colKey];
    pthread_mutex_unlock(&metadataMapLock);
    std::string server1 = std::get<0>(addrPair);
    std::string server2 = std::get<1>(addrPair);
    pthread_mutex_lock(&serverLoadMapLock);
    if(serverLoadMap[server1].type == BACKEND_PRIMARY){
        pthread_mutex_unlock(&serverLoadMapLock);
        return server1;
    }
    else{
        pthread_mutex_unlock(&serverLoadMapLock);
        return server2;
    }
}

// get both primary and replica address
bool getFullAddr(std::string rowKey, std::string colKey, std::string& fullAddr){
    pthread_mutex_lock(&metadataMapLock);
    std::pair<std::string, std::string> addrPair = metadataMap[rowKey][colKey];
    pthread_mutex_unlock(&metadataMapLock);
    std::string server1 = std::get<0>(addrPair);
    std::string server2 = std::get<1>(addrPair);
    if(server1.compare(EMPTY) == 0 || server2.compare(EMPTY) == 0){
        return false;
    }
    std::ostringstream oss;
    pthread_mutex_lock(&serverLoadMapLock);
    if(serverLoadMap[server1].type == BACKEND_PRIMARY && serverLoadMap[server1].wStatus == WORKING){
        oss << server1;
        if(serverLoadMap[server2].wStatus == WORKING)
            oss << "#" << server2;
    }
    else if(serverLoadMap[server2].type == BACKEND_PRIMARY && serverLoadMap[server2].wStatus == WORKING){
        oss << server2;
        if(serverLoadMap[server1].wStatus == WORKING)
            oss << "#" << server1;
    }
    fullAddr = oss.str();
    pthread_mutex_unlock(&serverLoadMapLock);
    return true;
}

// find username
bool findUserName(std::string userName){
    pthread_mutex_lock(&passwordMapLock);
    if(passwordMap.find(userName) == passwordMap.end()){
        pthread_mutex_unlock(&passwordMapLock);
        return false;
    }
    pthread_mutex_unlock(&passwordMapLock);
    return true;
}

// look for row value
bool findRowKey(std::string rowKey){
    //pthread_mutex_lock(&metadataMapLock);
    if(metadataMap.find(rowKey) == metadataMap.end()){
        //pthread_mutex_unlock(&metadataMapLock);
        return false;
    }
    //pthread_mutex_unlock(&metadataMapLock);
    return true;
}

// look for col value
bool findColKey(std::string rowKey, std::string colKey){
    //pthread_mutex_lock(&metadataMapLock);
    if(metadataMap.find(rowKey) == metadataMap.end()){
        //pthread_mutex_unlock(&metadataMapLock);
        return false;
    }
    else if(metadataMap[rowKey].find(colKey) == metadataMap[rowKey].end()){
        //pthread_mutex_unlock(&metadataMapLock);
        return false;
    }
    //pthread_mutex_unlock(&metadataMapLock);
    return true;
}

// remove a metadata table entry
void removeFile(std::string rowKey, std::string colKey){
    pthread_mutex_lock(&metadataMapLock);
    std::unordered_map<std::string, std::pair<std::string, std::string>>::iterator colIt = metadataMap[rowKey].find(colKey);
    metadataMap[rowKey].erase(colIt);
    if(metadataMap[rowKey].empty()){
        std::unordered_map<std::string, std::unordered_map<std::string, std::pair<std::string, std::string>>>::iterator rowIt = metadataMap.find(rowKey);
        metadataMap.erase(rowIt);
    }
    pthread_mutex_unlock(&metadataMapLock);
    return;
}

// add a metadata table entry
void addFile(std::string rowKey, std::string colKey, std::string value){
    /*
    pthread_mutex_lock(&metadataMapLock);
    if(findRowKey(rowKey))
        metadataMap[rowKey][colKey] = value;
    else{
        std::unordered_map<std::string, std::string> subMap;
        subMap[colKey] = value;
        metadataMap[rowKey] = subMap;
    }
    pthread_mutex_unlock(&metadataMapLock);
    return;
    */
    std::pair<std::string, std::string> addrPair;
    pthread_mutex_lock(&metadataMapLock);
    if(findColKey(rowKey, colKey)){
        //std::cout << "flag" << std::endl;
        addrPair = metadataMap[rowKey][colKey];
        if(std::get<0>(addrPair).compare(EMPTY) == 0)
            std::get<0>(addrPair) = value;
        else if(std::get<1>(addrPair).compare(EMPTY) == 0)
            std::get<1>(addrPair) = value;
        metadataMap[rowKey][colKey] = addrPair;
    }
    else if(findRowKey(rowKey)){
        //std::cout << "flag3" << std::endl;
        std::get<0>(addrPair) = value;
        std::get<1>(addrPair) = EMPTY;
        metadataMap[rowKey][colKey] = addrPair;
    }
    else{
        //std::cout << "flag5" << std::endl;
        std::get<0>(addrPair) = value;
        std::get<1>(addrPair) = EMPTY;
        std::unordered_map<std::string, std::pair<std::string, std::string>> subMap;
        subMap[colKey] = addrPair;
        metadataMap[rowKey] = subMap;
    }
    pthread_mutex_unlock(&metadataMapLock);
    return;
}

// build server status list for admin
std::string buildBackendStatus(){
    std::ostringstream oss;
    std::string statusStr;
    std::list<std::string>::iterator it;
    pthread_mutex_lock(&serverLoadMapLock);
    for(it=backendList.begin(); it!=backendList.end(); it++){
        oss << (serverLoadMap[*it].wStatus == WORKING) << "#";
    }
    pthread_mutex_unlock(&serverLoadMapLock);
    statusStr = oss.str();
    if(statusStr.compare(EMPTY) != 0)
        statusStr.pop_back();
    return statusStr;
}

// build table status
std::string buildTableInfo(){
    std::ostringstream oss;
    pthread_mutex_lock(&metadataMapLock);
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<std::string, std::string>>>::iterator rowIt;
    for(rowIt = metadataMap.begin(); rowIt != metadataMap.end(); rowIt ++){
        std::unordered_map<std::string, std::pair<std::string, std::string>> colMap = rowIt->second;
        std::unordered_map<std::string, std::pair<std::string, std::string>>::iterator colIt;
        for(colIt = colMap.begin(); colIt != colMap.end(); colIt ++){
            std::pair<std::string, std::string> addrPair = colIt->second;
            oss << rowIt->first << "#" << colIt->first << "#" << std::get<0>(addrPair) << ", " << std::get<1>(addrPair) << "#";
        }
    }
    pthread_mutex_unlock(&metadataMapLock);
    std::string tableInfo = oss.str();
    if(tableInfo.compare(EMPTY) != 0)
        tableInfo.pop_back();
    return tableInfo;
}

// build view list according to file type
std::string buildView(std::string userName, std::string fileType){
    //std::cout << "flag1" << std::flush;
    pthread_mutex_lock(&metadataMapLock);
    std::ostringstream oss;
    oss << "#" << fileType;
    std::string key = oss.str();
    oss.clear();
    oss.str("");
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<std::string, std::string>>>::iterator rowIt;
    //std::cout << "flag2" <<std::flush;
    for(rowIt = metadataMap.begin(); rowIt != metadataMap.end(); rowIt ++){
        //std::cout << rowIt->first << std::endl << std::flush;
        if((rowIt->first).compare(userName) != 0)
            continue;
        std::unordered_map<std::string, std::pair<std::string, std::string>> colMap = rowIt->second;
        std::unordered_map<std::string, std::pair<std::string, std::string>>::iterator colIt;
        int sharpInd;
        for(colIt = colMap.begin(); colIt != colMap.end(); colIt ++){
            //std::cout << colIt->first << std::endl << std::flush;
            if((sharpInd = (colIt->first).find(key)) != std::string::npos){
                //std::string addr = (colIt->first).substr(0, sharpInd);
                oss << colIt->first + "#";
                //std::cout << addr << std::flush;
            }
        }
    }
    pthread_mutex_unlock(&metadataMapLock);
    if(fileType.compare("DRIVE") == 0){
        oss << buildView(userName, "FOLDER");
    }
    std::string view = oss.str();
    if(fileType.compare("DRIVE") == 0 && view.compare(EMPTY) != 0)
        view.pop_back();
    return view;
}

// check email type from the receiver's address
EMAILTYPE getEmailType(std::string receiverAddr){
    if(receiverAddr.find("@gmail.com") != std::string::npos)
        return REMOTE;
    if(receiverAddr.find("@penncloud.com") != std::string::npos)
        return LOCAL;
    return OTHER;
}

// print metadata table
void printMetadataTable(){
    pthread_mutex_lock(&metadataMapLock);
    std::unordered_map<std::string, std::unordered_map<std::string, std::pair<std::string, std::string>>>::iterator rowIt;
    for(rowIt = metadataMap.begin(); rowIt != metadataMap.end(); rowIt ++){
        std::unordered_map<std::string, std::pair<std::string, std::string>> colMap = rowIt->second;
        std::unordered_map<std::string, std::pair<std::string, std::string>>::iterator colIt;
        for(colIt = colMap.begin(); colIt != colMap.end(); colIt ++){
            std::cout << rowIt->first << "  " << colIt->first << "  1: " << std::get<0>(colIt->second) << "  2: "<< std::get<1>(colIt->second) << std::endl;
        }
    }
    pthread_mutex_unlock(&metadataMapLock);
    return;
}

// print serverLoad table
void printServerLoadTable(){
    pthread_mutex_lock(&serverLoadMapLock);
    std::unordered_map<std::string, struct serverStatus>::iterator rowIt;
    for(rowIt = serverLoadMap.begin(); rowIt != serverLoadMap.end(); rowIt ++){
        struct serverStatus status = rowIt->second;
        std::cout << rowIt->first << "  " << status.type << "  " << status.wStatus << "  " << status.load << std::endl;
    }
    pthread_mutex_unlock(&serverLoadMapLock);
    return;
}

// thread routine
void* workerRoutine(void* task){
    printDebug("NEW CONNECTION", OBJECTIVE, UNKNOWN, EMPTY);
    //printServerLoadTable();
    threadTask* tTask = (threadTask *)task;
    int clientFd = tTask->clientFd;
    std::string thisAddr;
    char recvBuffer[MAXLEN];
    int len;
    if((len = recv(clientFd, recvBuffer, MAXLEN, 0)) < 0)
        printError("Recv Client Type Failed: ", ERRCODE);
    recvBuffer[len] = '\0';
    thisAddr = strTrim(recvBuffer);
    printDebug(recvBuffer, D_RECV, getClientType(thisAddr), thisAddr);
    //printServerLoadTable();
    pthread_mutex_lock(&serverLoadMapLock);
    if(serverLoadMap.find(thisAddr) == serverLoadMap.end() || serverLoadMap[thisAddr].wStatus != CRASH){
        pthread_mutex_unlock(&serverLoadMapLock);
        sendMsg(clientFd, "+OK");
        printDebug("+OK", D_SEND, getClientType(thisAddr), thisAddr);
    }
    else{
        pthread_mutex_unlock(&serverLoadMapLock);
        pthread_mutex_lock(&replicaMapLock);
        std::string logAddr = replicaMap[thisAddr];
        pthread_mutex_unlock(&replicaMapLock);
        sendMsg(clientFd, logAddr);
        printDebug(logAddr, D_SEND, getClientType(thisAddr), thisAddr);
    }
    initClientType(thisAddr);
    //printServerLoadTable();
    while(1){
        // recv msg from other server
        if((len = recv(clientFd, recvBuffer, MAXLEN, 0)) < 0)
            printError("Recv Failed: ", ERRCODE);
        if(len == 0){
            handleCrashServer(thisAddr, getClientType(thisAddr));
            printDebug("DISCONNECTED", OBJECTIVE, getClientType(thisAddr), thisAddr);
            close(clientFd);
            pthread_exit(NULL);
        }
        recvBuffer[len] = '\0';
        //std::cout << recvBuffer << std::endl;
        printDebug(recvBuffer, D_RECV, getClientType(thisAddr), thisAddr);
        // check and parse the cmd
        std::string msg = recvBuffer;
        CMD cmd = getCmd(msg);
        std::vector<std::string> argVec = parseCmd(msg);
        std::ostringstream keyBuilder;
        std::ostringstream replyBuilder;
        std::string userName = argVec[0], password, fileAddr, oldFileAddr, fileType, colKey, serverAddr, receiverAddr, reply;
        EMAILTYPE emailType;
        switch(cmd){
        case REG:
            password = argVec[1];
            if(findUserName(userName))
                reply = "-ERR user name already registered";
            else{
                reply = "+OK";
                pthread_mutex_lock(&passwordMapLock);
                passwordMap[userName] = password;
                pthread_mutex_unlock(&passwordMapLock);
                struct userStatus status;
                status.online = false;
                status.lastActiveTime = 0;
                pthread_mutex_lock(&onlineMapLock);
                onlineMap[userName] = status;
                pthread_mutex_unlock(&onlineMapLock);
            }
            break;
        case LOGIN:
            password = argVec[1];
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else if(passwordMap[userName].compare(password) != 0)
                reply = "-ERR password incorrect";
            else if(onlineMap[userName].online)
                reply = "-ERR user already logged in";
            else{
                reply = "+OK";
                pthread_mutex_lock(&onlineMapLock);
                onlineMap[userName].online = true;
                onlineMap[userName].lastActiveTime = getCurrTime();
                pthread_mutex_unlock(&onlineMapLock);
            }
            break;
        case LOGOUT:
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else if(!onlineMap[userName].online)
                reply = "-ERR user not logged in";
            else{
                reply = "+OK";
                pthread_mutex_lock(&onlineMapLock);
                onlineMap[userName].online = false;
                pthread_mutex_unlock(&onlineMapLock);
            }
            break;
        case PASS:
            password = argVec[1];
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else{
                reply = "+OK";
                pthread_mutex_lock(&passwordMapLock);
                passwordMap[userName] = password;
                pthread_mutex_unlock(&passwordMapLock);
                pthread_mutex_lock(&onlineMapLock);
                onlineMap[userName].online = false;
                pthread_mutex_unlock(&onlineMapLock);
            }
            break;
        case GET:
            fileAddr = argVec[1];
            fileType = argVec[2];
            keyBuilder << fileAddr << "#" << fileType;
            colKey = keyBuilder.str();
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else if(!findRowKey(userName))
                reply = "-ERR file not found";
            else if(!findColKey(userName, colKey))
                reply = "-ERR file not found";
            else{
                //serverAddr = metadataMap[userName][colKey];
                serverAddr = getPrimaryAddr(userName, colKey);
                replyBuilder << "+OK " << serverAddr;
                reply = replyBuilder.str();
                incrLoad(serverAddr);
                updateTime(userName);
            }
            break;
        case DEL:
            fileAddr = argVec[1];
            fileType = argVec[2];
            keyBuilder << fileAddr << "#" << fileType;
            colKey = keyBuilder.str();
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else if(!findRowKey(userName))
                reply = "-ERR file not found";
            else if(!findColKey(userName, colKey))
                reply = "-ERR file not found";
            else{
                //serverAddr = metadataMap[userName][colKey];
                while(!getFullAddr(userName, colKey, serverAddr)){
                    std::cout << "DEADLOCK!!!\n";
                    sleep(1);
                }
                replyBuilder << "+OK " << serverAddr;
                reply = replyBuilder.str();
                incrLoad(serverAddr);
                updateTime(userName);
            }
            break;
        case PUT:
            fileAddr = argVec[1];
            fileType = argVec[2];
            keyBuilder << fileAddr << "#" << fileType;
            colKey = keyBuilder.str();
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else{
                serverAddr = findAvailableServer();
                if(serverAddr.compare(EMPTY) != 0){
                    replyBuilder << "+OK " << serverAddr;
                    reply = replyBuilder.str();
                    incrLoad(serverAddr);
                    updateTime(userName);
                }
                else
                    reply = "-ERR no available server";
            }
            break;
        case MOVE:
            oldFileAddr = argVec[1];
            fileType = argVec[3];
            keyBuilder << oldFileAddr << "#" << fileType;
            colKey = keyBuilder.str();
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else if(!findRowKey(userName))
                reply = "-ERR file not found";
            else if(!findColKey(userName, colKey))
                reply = "-ERR file not found";
            else{
                serverAddr = findAvailableServer();
                if(serverAddr.compare(EMPTY) != 0){
                    replyBuilder << "+OK " << serverAddr;
                    reply = replyBuilder.str();
                    incrLoad(serverAddr);
                    updateTime(userName);
                }
                else
                    reply = "-ERR no available server";
            }
            break;
        case DIR:
            fileAddr = argVec[1];
            fileType = argVec[2];
            keyBuilder << fileAddr << "#" << fileType;
            colKey = keyBuilder.str();
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else if(findRowKey(userName) && findColKey(userName, colKey))
                reply = "-ERR directory already exists";
            else{
                serverAddr = findAvailableServer();
                if(serverAddr.compare(EMPTY) != 0){
                    replyBuilder << "+OK " << serverAddr;
                    reply = replyBuilder.str();
                    incrLoad(serverAddr);
                    updateTime(userName);
                }
                else
                    reply = "-ERR no available server";
            }
            break;
        case VIEW:
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else{
                fileType = argVec[1];
                replyBuilder << "+OK ";
                replyBuilder << buildView(userName, fileType);
                reply = replyBuilder.str();
                updateTime(userName);
            }
            break;
        case STATE:
            replyBuilder << "+OK ";
            replyBuilder << buildBackendStatus();
            reply = replyBuilder.str();
            break;
        case TABLE:
            replyBuilder << "+OK ";
            replyBuilder << buildTableInfo();
            reply = replyBuilder.str();
            break;
        case SEND:
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else{
                receiverAddr = argVec[1];
                emailType = getEmailType(receiverAddr);
                if(emailType == REMOTE){
                    replyBuilder << "+OK REMOTE#" << smtpClientAddr;
                    reply = replyBuilder.str();
                    updateTime(userName);
                }
                else if(emailType == LOCAL){
                    serverAddr = findAvailableServer();
                    if(serverAddr.compare(EMPTY) != 0){
                        replyBuilder << "+OK LOCAL#" << serverAddr;
                        reply = replyBuilder.str();
                        incrLoad(serverAddr);
                        updateTime(userName);
                    }
                    else
                        reply = "-ERR no available server";
                }
                else
                    reply = "-ERR";
            }   
            break;
        case SPUT:
        case SDIR:
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else{
                fileAddr = argVec[1];
                fileType = argVec[2];
                serverAddr = argVec[3];
                keyBuilder << fileAddr << "#" << fileType;
                colKey = keyBuilder.str();
                addFile(userName, colKey, serverAddr);
                reply = "+OK";
            }
            decrLoad(serverAddr);
            break;
        case SDEL:
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else{
                fileAddr = argVec[1];
                fileType = argVec[2];
                serverAddr = argVec[3];
                keyBuilder << fileAddr << "#" << fileType;
                colKey = keyBuilder.str();
                if(!findRowKey(userName))
                    reply = "-ERR file not found";
                else if(!findColKey(userName, colKey))
                    reply = "-ERR file not found";
                else{
                    removeFile(userName, colKey);
                    reply = "+OK";
                }
            }
            decrLoad(serverAddr);
            break;
        case SMOVE:
            if(!findUserName(userName))
                reply = "-ERR user name not found";
            else{
                oldFileAddr = argVec[1];
                fileAddr = argVec[2];
                fileType = argVec[3];
                serverAddr = argVec[4];
                keyBuilder << oldFileAddr << "#" << fileType;
                colKey = keyBuilder.str();
                if(!findRowKey(userName))
                    reply = "-ERR file not found";
                else if(!findColKey(userName, colKey))
                    reply = "-ERR file not found";
                else{
                    //serverAddr = metadataMap[userName][colKey];
                    removeFile(userName, colKey);
                    keyBuilder.clear();
                    keyBuilder.str("");
                    keyBuilder << fileAddr << "#" << fileType;
                    colKey = keyBuilder.str();
                    addFile(userName, colKey, serverAddr);
                    pthread_mutex_lock(&replicaMapLock);
                    addFile(userName, colKey, replicaMap[serverAddr]);
                    pthread_mutex_unlock(&replicaMapLock);
                    reply = "+OK";
                }
            }
            decrLoad(serverAddr);
            break;
        case FINISH:
            reply = "+OK";
            serverAddr = argVec[0];
            decrLoad(serverAddr);
            break;
        default:
            reply = "-ERR Invalid Command";
            break;
        }
        //std::cout << reply << std::endl;
        std::cout << "********Metadata Table********\n";
        printMetadataTable();
        std::cout << "******************************\n";
        std::cout << "*******Server Load Info*******\n";
        printServerLoadTable();
        std::cout << "******************************\n";
        sendMsg(clientFd, reply);
        printDebug(reply, D_SEND, getClientType(thisAddr), thisAddr);
    }

    pthread_exit(NULL);
    return NULL;
}

int main(int argc, char *argv[]){
    // parse command
    int opt;
    std::string configPath = EMPTY;
    int port = 0;
    bool cFlag = false;
    bool vFlag = false;
    bool pFlag = false;
    while ((opt = getopt(argc, argv, "c:p:v")) != -1){
        switch(opt){
        case 'c':
            cFlag = true;
            configPath = optarg;
            break;
        case 'p':
            pFlag = true;
            port = atoi(optarg);
            break;
        case 'v':
            vFlag = true;
            break;
        default:
            printError("Usage: ./MasterServer -c ConfigFilePath -p Port [-v] \n", ERRCODE);
            break;
        }
    }
    if(configPath.compare(EMPTY) == 0 || port == 0 || argc-optind != 0){
        printError("Usage: ./MasterServer -c ConfigFilePath -p Port [-v] \n", ERRCODE);
    }
    debug = vFlag;
    initFromConfig(configPath);
    //initTimer();
    initLock();

    // variables for socket
    int serverFd, clientFd;
    struct sockaddr_in serverAddr;
    socklen_t sockaddrSize = sizeof(struct sockaddr);
    struct sockaddr_in clientAddr;
    socklen_t clientAddrSize = sizeof(struct sockaddr);

    // variables for worker thread
    pthread_attr_t attr;

    // initialize sockets and related variables
    if((serverFd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        printError("Socket Init Failed: ", ERRCODE);
    serverAddr.sin_family=AF_INET;
    serverAddr.sin_port=htons(port);
    serverAddr.sin_addr.s_addr=INADDR_ANY;
    bzero(&(serverAddr.sin_zero),8);

    // bind
    if(bind(serverFd,(struct sockaddr *)&serverAddr, sockaddrSize) < 0)
        printError("Bind Failed: ", ERRCODE);

    // listen
    if(listen(serverFd, MAXCONNECTION) < 0)
        printError("Listen Failed: ", ERRCODE);

    // accept connection and create thread to handle request
    pthread_attr_init(&attr);
    while(1){
        pthread_t thread;
        if((clientFd = accept(serverFd, (struct sockaddr *)&clientAddr, &clientAddrSize)) < 0){
            printError("Accept Failed: ", ERRCODE);
        }
        struct threadTask task;
        task.clientFd = clientFd;
        pthread_create(&thread, &attr, workerRoutine, &task);
    }

    //close socket
    close(serverFd);
    return 0;
}
