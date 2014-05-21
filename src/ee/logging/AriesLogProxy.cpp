/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "AriesLogProxy.h"
#include "execution/VoltDBEngine.h"
#include <string>

using std::ios;
using std::string;
using std::cout;
using std::endl;

using namespace voltdb;

//XXX Must match with HStoreSite
string AriesLogProxy::defaultLogfileName = "aries.log";

AriesLogProxy::AriesLogProxy(VoltDBEngine *engine) {
	init(engine, defaultLogfileName);
}

AriesLogProxy::AriesLogProxy(VoltDBEngine *engine, string logfileName) {
	init(engine, logfileName);
}

void AriesLogProxy::init(VoltDBEngine *engine, string logfileName) {
	this->logFileName = logfileName;
	// XXX originally true
	jniLogging = false;

	if (!jniLogging) {
		// append + binary mode
		logFile = fopen(logfileName.c_str(), "ab+");
		logFileFD = fileno(logFile);

		if(logFile != NULL){
			VOLT_DEBUG("AriesLogProxy : opened logfile %s ", logFileName.c_str());
		}
		else{
			VOLT_ERROR("AriesLogProxy : cannot open logfile %s ", logFileName.c_str());
		}
	} else {
		if (engine == NULL) {
			cout << "what in the god's name is this shit " << endl;
		}
		this->engine = engine;
	}
}

AriesLogProxy::~AriesLogProxy() {
	if(logFile != NULL){
		int ret = fclose(logFile);

		if(ret == 0){
			VOLT_DEBUG("AriesLogProxy : closed logfile %s", logFileName.c_str());
		}
		else{
			VOLT_ERROR("AriesLogProxy : could not close logfile %s", logFileName.c_str());
		}
	}
}

AriesLogProxy* AriesLogProxy::getAriesLogProxy(VoltDBEngine *engine) {
	if(!engine || !engine->isARIESEnabled()) {
		VOLT_DEBUG("AriesLogProxy : NULL");
		return NULL;
	}

	if (!(engine->getARIESFile().empty())) {
		VOLT_DEBUG("AriesLogProxy : logfile %s", engine->getARIESFile().c_str());
		return new AriesLogProxy(engine, engine->getARIESFile());
	}

	VOLT_DEBUG("AriesLogProxy : NULL");
	return NULL;
}

string AriesLogProxy::getLogFileName() {
	return logFileName;
}

void AriesLogProxy::log(LoggerId loggerId, LogLevel level, const char *statement) const {
	string loggerName;

	switch (loggerId) {
	case voltdb::LOGGERID_HOST:
		loggerName = "HOST";
		break;
	case voltdb::LOGGERID_SQL:
		loggerName = "SQL";
		break;
//#ifdef ARIES
	case voltdb::LOGGERID_MM_ARIES:
		loggerName = "MM_ARIES";
		break;
//#endif
	default:
		loggerName = "UNKNOWN";
		break;
	}

	std::string logLevel;
	switch (level) {
	case LOGLEVEL_ALL:
		logLevel = "ALL";
		break;
	case LOGLEVEL_TRACE:
		logLevel = "TRACE";
		break;
	case LOGLEVEL_DEBUG:
		logLevel = "DEBUG";
		break;
	case LOGLEVEL_INFO:
		logLevel = "INFO";
		break;
	case LOGLEVEL_WARN:
		logLevel = "WARN";
		break;
	case LOGLEVEL_ERROR:
		logLevel = "ERROR";
		break;
	case LOGLEVEL_FATAL:
		logLevel = "FATAL";
		break;
	case LOGLEVEL_OFF:
		logLevel = "OFF";
		break;
	default:
		logLevel = "UNKNOWN";
		break;
	}

	// log this stuff to stdout, do not mix with aries log.
    cout << loggerName << " - " << logLevel << " - " << statement << endl;
}

void AriesLogProxy::logBinaryOutput(const char *data, size_t size) {
	if (jniLogging) {
		VOLT_DEBUG("AriesLogProxy : logToEngineBuffer : %lu", size);
		logToEngineBuffer(data, size);
	} else {
		VOLT_DEBUG("AriesLogProxy : logLocally : %lu", size);
		logLocally(data, size);
	}
}

void AriesLogProxy::logLocally(const char *data, size_t size) {
	size_t s_ret = -1;
	int ret = -1;

	s_ret = fwrite (data , sizeof(char), size, logFile);
	if(s_ret != size){
		VOLT_ERROR("logLocally failed : badbit set");
	}

	ret = fflush(logFile);
	if(ret == 0){
	 	VOLT_DEBUG("logLocally : flushed : file pos %ld", ftell(logFile));
	}
	else{
	 	VOLT_ERROR("logLocally : flushed : file pos %ld", ftell(logFile));
	}

	// SYNC changes
	ret = fsync(logFileFD);
	if(ret == 0){
		VOLT_DEBUG("logLocally : synced file");
	}
	else{
		VOLT_ERROR("logLocally : could not sync file ");
	}

}

void AriesLogProxy::logToEngineBuffer(const char *data, size_t size) {
#ifdef ARIES
	//engine->writeToAriesLogBuffer(data, size);
#endif
}
