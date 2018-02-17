#ifndef LOGGER_H_
#define LOGGER_H_

#include <iostream>
#include <cstring>
#include <pthread.h>

using namespace std;

class Logger{

	std::string logFile;
	pthread_mutex_t logLock;

public:
	Logger(std::string file);
	bool appendLog(std::string entry);
	bool getLog(std::string& logContent);
	bool clearLog();
};

#endif





