/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#ifndef LOGMANAGER_H_
#define LOGMANAGER_H_
#include "Logger.h"
#include "LogDefs.h"
#include "LogProxy.h"
#include <stdint.h>
#include <iostream>
#include <pthread.h>

#include "common/debuglog.h"

namespace voltdb {

/**
 * A LogManager contains a hard coded set of loggers that have counterpart loggers elsewhere.
 */
class LogManager {
public:

	LogManager(LogProxy *proxy);

	/**
	 * Constructor that initializes all the loggers with the specified proxy
	 * @param proxy The LogProxy that all the loggers should use
	 * @param engine
	 */
	LogManager(LogProxy *proxy, VoltDBEngine *engine);

    /**
     * Retrieve a logger by ID
     * @parameter loggerId ID of the logger to retrieve
     */
    inline const Logger* getLogger(LoggerId id) const {
        switch (id) {
        case LOGGERID_SQL:
            return &m_sqlLogger;
        case LOGGERID_HOST:
            return &m_hostLogger;
        case LOGGERID_MM_ARIES:
            return &m_ariesLogger;
        default:
            return NULL;
        }
    }

    /**
     * Update the log levels of the loggers.
     * @param logLevels Integer contaning the log levels for the various loggers
     */
    inline void setLogLevels(int64_t logLevels) {
        m_sqlLogger.m_level = static_cast<LogLevel>((7 & logLevels));
        m_hostLogger.m_level = static_cast<LogLevel>(((7 << 3) & logLevels) >> 3);

        m_ariesLogger.m_level = static_cast<LogLevel>(LOGLEVEL_TRACE);
    }

    /**
     * Retrieve the log proxy used by this LogManager and its Loggers
     * @return LogProxy Pointer to the LogProxy in use by this LogManager and its Loggers
     */
    inline const LogProxy* getLogProxy() {
        return m_proxy;
    }

    void setAriesProxyEngine(VoltDBEngine*);

    /**
     * Frees the log proxy
     */
    ~LogManager() {
        delete m_proxy;
    }


    /**
     * Retrieve a logger by ID from the LogManager associated with this thread.
     * @parameter loggerId ID of the logger to retrieve
     */
    inline const Logger* getThreadLogger(LoggerId id) {
    	// XXX remove static stuff
    	//LogManager* manager = getThreadLogManager();

    	LogManager* manager = this;
    	assert(manager != NULL);

    	const Logger* logger = manager->getLogger(id);
    	assert(logger != NULL);

    	//VOLT_WARN("Thread id : %lu",pthread_self());
    	//VOLT_WARN("LogManager : %p Logger : %p", manager, logger);

        return logger;
    }

    Logger getAriesLogger(){
    	return (m_ariesLogger);
    }


private:

    static LogManager* getThreadLogManager();

    /**
     * The log proxy in use by this LogManager and its Loggers
     */
    const LogProxy *m_proxy;
    Logger m_sqlLogger;
    Logger m_hostLogger;
    Logger m_ariesLogger;
};
}
#endif /* LOGMANAGER_H_ */
