
#include "Logger.h"
#include <fstream>
#include <sstream>

Logger::Logger(std::string file){
    logFile = file;
    logLock = PTHREAD_MUTEX_INITIALIZER;
};


bool Logger::appendLog(std::string entry){
    pthread_mutex_lock(&logLock);
    std::ofstream ofs;
    ofs.open(logFile, std::ios_base::app);
    ofs << entry << ",";
    ofs.close();
    pthread_mutex_unlock(&logLock);
    return true;
}

bool Logger::getLog(std::string& logContent){
    pthread_mutex_lock(&logLock);
    std::ifstream ifs(logFile);
    std::stringstream sstr;
    while(ifs >> sstr.rdbuf());
    ifs.close();
    logContent = sstr.str();
    pthread_mutex_unlock(&logLock);
    return true;
}

bool Logger::clearLog(){
    pthread_mutex_lock(&logLock);
    std::ofstream ofs;
    ofs.open(logFile, std::ofstream::out | std::ofstream::trunc);
    ofs.close();
    pthread_mutex_unlock(&logLock);
    return true;
}