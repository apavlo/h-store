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

#ifndef UNDOLOG_H_
#define UNDOLOG_H_
#include <vector>
#include <deque>
#include <stdint.h>
#include "common/debuglog.h"
#include "common/Pool.hpp"
#include "common/UndoQuantum.h"
#include "boost/pool/object_pool.hpp"
#include <iostream>

namespace voltdb
{
    class UndoLog
    {
    public:
        UndoLog();
        virtual ~UndoLog();
        /**
         * Clean up all outstanding state in the UndoLog.  Essentially
         * contains all the work that should be performed by the
         * destructor.  Needed to work around a memory-free ordering
         * issue in VoltDBEngine's destructor.
         */
        void clear();

        inline UndoQuantum* generateUndoQuantum(int64_t nextUndoToken) {
            VOLT_TRACE("Generating token %ld / lastUndo:%ld / lastRelease:%ld / undoQuantums:%ld",
                       (long int)nextUndoToken, (long int)m_lastUndoToken, (long int)m_lastReleaseToken, (long int)m_undoQuantums.size());
            
            // Since ExecutionSite is using monotonically increasing
            // token values, every new quanta we're asked to generate should be
            // larger than any token value we've seen before
            #ifdef VOLT_ERROR_ENABLED
            if (nextUndoToken <= m_lastUndoToken) {
                VOLT_ERROR("nextUndoToken[%ld] is less than or equal to m_lastUndoToken[%ld]",
                           (long int)nextUndoToken, (long int)m_lastUndoToken);
            }
            #endif
            assert(nextUndoToken > m_lastUndoToken);

            #ifdef VOLT_ERROR_ENABLED
            if (nextUndoToken <= m_lastReleaseToken) {
                VOLT_ERROR("nextUndoToken[%ld] is less than or equal to m_lastReleaseToken[%ld]",
                           (long int)nextUndoToken, (long int)m_lastReleaseToken);
            }
            #endif
            assert(nextUndoToken > m_lastReleaseToken);
            
            m_lastUndoToken = nextUndoToken;
            Pool *pool = NULL;
            if (m_undoDataPools.size() == 0) {
                pool = new Pool();
            } else {
                pool = m_undoDataPools.back();
                m_undoDataPools.pop_back();
            }
            assert(pool);
            UndoQuantum *undoQuantum =
                new (pool->allocate(sizeof(UndoQuantum)))
                UndoQuantum(nextUndoToken, pool);
            m_undoQuantums.push_back(undoQuantum);
            VOLT_TRACE("Created new UndoQuantum %ld", (long int)nextUndoToken);
            return undoQuantum;
        }

        /*
         * Undo all undoable actions from the latest undo quantum back
         * until the undo quantum with the specified undo token.
         */
        inline void undo(const int64_t undoToken) {
            VOLT_TRACE("Attempting to undo token %ld [lastUndo:%ld / lastRelease:%ld / undoQuantums:%ld]",
                       undoToken, m_lastUndoToken, m_lastReleaseToken, m_undoQuantums.size());
            
            // This ensures that we don't attempt to undo something in
            // the distant past.  In some cases ExecutionSite may hand
            // us the largest token value that definitely doesn't
            // exist; this will just result in all undo quanta being undone.
            #ifdef VOLT_ERROR_ENABLED
            if (undoToken < m_lastReleaseToken) {
                VOLT_ERROR("undoToken[%ld] is less than m_lastReleaseToken[%ld]",
                           (long int)undoToken, (long int)m_lastReleaseToken);
            }
            #endif
            assert(undoToken >= m_lastReleaseToken);

            if (undoToken > m_lastUndoToken) {
                // a procedure may abort before it sends work to the EE
                // (informing the EE of its undo token. For example, it
                // may have invalid parameter values or, possibly, aborts
                // in user java code before executing any SQL.  Just
                // return. There is no work to do here.
                return;
            }

            m_lastUndoToken = undoToken - 1;
            while (m_undoQuantums.size() > 0) {
                UndoQuantum *undoQuantum = m_undoQuantums.back();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                if (undoQuantumToken < undoToken) {
                    VOLT_TRACE("Skipping UndoQuantum %ld because it is before token %ld",
                               undoQuantumToken, undoToken);
                    return;
                }

                VOLT_TRACE("START Aborting UndoQuantum %ld", undoQuantumToken);
                m_undoQuantums.pop_back();
                Pool *pool = undoQuantum->getDataPool();
                undoQuantum->undo();
                pool->purge();
                m_undoDataPools.push_back(pool);

                if (undoQuantumToken == undoToken) {
                    VOLT_TRACE("FINISH Aborting UndoQuantum %ld", undoQuantumToken);
                    return;
                }
            }
            VOLT_WARN("Unable to find token %ld [lastUndo:%ld / lastRelease:%ld / undoQuantums:%ld]",
                       undoToken, m_lastUndoToken, m_lastReleaseToken, m_undoQuantums.size());
        }
        
        /*
         * Undo all undoable actions for the given range
         */
        inline void undoRange(const int64_t startToken, const int64_t stopToken) {
            assert(startToken >= stopToken);
            
            VOLT_TRACE("Attempting to undo range %ld->%ld [lastUndo:%ld / lastRelease:%ld / undoQuantums:%ld]",
                       startToken, stopToken,
                       m_lastUndoToken, m_lastReleaseToken, m_undoQuantums.size());
            
            // This ensures that we don't attempt to undo something in
            // the distant past.  In some cases ExecutionSite may hand
            // us the largest token value that definitely doesn't
            // exist; this will just result in all undo quanta being undone.
            #ifdef VOLT_ERROR_ENABLED
            if (startToken < m_lastReleaseToken) {
                VOLT_ERROR("startToken[%ld] is less than m_lastReleaseToken[%ld]",
                           (long int)startToken, (long int)m_lastReleaseToken);
            }
            if (stopToken < m_lastReleaseToken) {
                VOLT_ERROR("stopToken[%ld] is less than m_lastReleaseToken[%ld]",
                           (long int)stopToken, (long int)m_lastReleaseToken);
            }
            #endif
            assert(startToken >= m_lastReleaseToken);
            assert(stopToken >= m_lastReleaseToken);

            if (startToken > m_lastUndoToken) {
                // a procedure may abort before it sends work to the EE
                // (informing the EE of its undo token. For example, it
                // may have invalid parameter values or, possibly, aborts
                // in user java code before executing any SQL.  Just
                // return. There is no work to do here.
                return;
            }

            while (m_undoQuantums.size() > 0) {
                UndoQuantum *undoQuantum = m_undoQuantums.back();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                
                // If this token is *before* our stop token, then we know that we've
                // successfully gone through the entire range that we were looking
                if (undoQuantumToken < stopToken) {
                    VOLT_TRACE("Halting search at UndoQuantum %ld because it is before stop token %ld",
                               undoQuantumToken, stopToken);
                    break;
                }
                
                m_undoQuantums.pop_back();
                
                // If this token is *after* our start token, then we haven't found the 
                // starting point of the range we're looking for. We'll put this token
                // into a temporary queue so that we can add it back after remove our range
                if (undoQuantumToken > startToken) {
                    VOLT_TRACE("Skipping UndoQuantum %ld because it is after start token %ld",
                               undoQuantumToken, startToken);
                    m_temp.push_back(undoQuantum);
                    continue;
                }

                // Here we go! We have something in the range that we are looking for
                VOLT_TRACE("START Aborting UndoQuantum %ld", undoQuantumToken);
                Pool *pool = undoQuantum->getDataPool();
                undoQuantum->undo();
                pool->purge();
                m_undoDataPools.push_back(pool);

                if (undoQuantumToken == stopToken) {
                    VOLT_TRACE("FINISH Aborting UndoQuantum %ld", undoQuantumToken);
                    break;
                }
            } // WHILE
            // Make sure that we add back the tokens that were outside of our range
            // in the right order.
            if (m_temp.size() != 0) {
                do {
                    UndoQuantum *undoQuantum = m_temp.back();
                    m_temp.pop_back();
                    m_undoQuantums.push_back(undoQuantum);
                } while (m_temp.size() > 0);
            }
            else {
                VOLT_WARN("Unable to find tokens in range %ld->%ld "\
                          "[lastUndo:%ld / lastRelease:%ld / undoQuantums:%ld]",
                          startToken, stopToken,
                          m_lastUndoToken, m_lastReleaseToken, m_undoQuantums.size());
            }
            m_lastUndoToken = m_undoQuantums.back()->getUndoToken();
        }

        /*
         * Release memory held by all undo quantums up to and
         * including the quantum with the specified token. It will be
         * impossible to undo these actions in the future.
         */
        inline void release(const int64_t undoToken) {
            VOLT_TRACE("Attempting to release token %ld [lastUndo:%ld / lastRelease:%ld / undoQuantums:%ld]",
                       undoToken, m_lastUndoToken, m_lastReleaseToken, m_undoQuantums.size());
            #ifdef VOLT_ERROR_ENABLED
            if (m_lastReleaseToken >= undoToken) {
                VOLT_ERROR("m_lastReleaseToken[%ld] is greater than or equal to undoToken[%ld]",
                           (long int)m_lastReleaseToken, (long int)undoToken);
            }
            #endif
            assert(m_lastReleaseToken < undoToken);
            m_lastReleaseToken = undoToken;
            while (m_undoQuantums.size() > 0) {
                UndoQuantum *undoQuantum = m_undoQuantums.front();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                if (undoQuantumToken > undoToken) {
                    VOLT_TRACE("Skipping UndoQuantum %ld because it is after token %ld",
                               undoQuantumToken, undoToken);
                    return;
                }

                VOLT_TRACE("START Releasing UndoQuantum %ld", undoQuantumToken);
                m_undoQuantums.pop_front();
                Pool *pool = undoQuantum->getDataPool();
                undoQuantum->release();
                pool->purge();
                m_undoDataPools.push_back(pool);
                if (undoQuantumToken == undoToken) {
                    VOLT_TRACE("FINISH Releasing UndoQuantum %ld", undoQuantumToken);
                    return;
                }
            }
            VOLT_WARN("Unable to find token %ld [lastUndo:%ld / lastRelease:%ld / undoQuantums:%ld]",
                       undoToken, m_lastUndoToken, m_lastReleaseToken, m_undoQuantums.size());
        }

    private:
        // These two values serve no real purpose except to provide
        // the capability to assert various properties about the undo tokens
        // handed to the UndoLog.  Currently, this makes the following
        // assumptions about how the Java side is managing undo tokens:
        //
        // 1. Java is generating monotonically increasing undo tokens.
        // There may be gaps, but every new token to generateUndoQuantum is
        // larger than every other token previously seen by generateUndoQuantum
        //
        // 2. Right now, the ExecutionSite _always_ releases the
        // largest token generated during a transaction at the end of the
        // transaction, even if the entire transaction was rolled back.  This
        // means that release may get called even if there are no undo quanta
        // present.

        // m_lastUndoToken is the largest token that could possibly be called
        // for real undo; any larger token is either undone or has
        // never existed
        int64_t m_lastUndoToken;

        // m_lastReleaseToken is the largest token that definitely
        // doesn't exist; any smaller value has already been released,
        // any larger value might exist (gaps are possible)
        int64_t m_lastReleaseToken;

        std::vector<Pool*> m_undoDataPools;
        std::deque<UndoQuantum*> m_undoQuantums;
        std::deque<UndoQuantum*> m_temp;
    };
}
#endif /* UNDOLOG_H_ */
