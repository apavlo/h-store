/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package edu.brown.api.results;

import org.apache.log4j.Logger;
import org.voltdb.utils.Pair;

import edu.brown.api.BenchmarkInterest;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;

public class ResultsChecker extends EventObservable<String> implements BenchmarkInterest {
    private static final Logger LOG = Logger.getLogger(ResultsChecker.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private long lastDelta = -1;
    private boolean stop = false;
    
    public ResultsChecker(EventObserver<String> failure_observer) {
        this.addObserver(failure_observer);
    }
    
    @Override
    public void stop() {
        this.stop = true;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults results) {
        // Nothing to do
        return (null);
    }
    
    @Override
    public void benchmarkHasUpdated(BenchmarkResults results) {
        if (this.stop) return;
        
        Pair<Long, Long> p = results.computeTotalAndDelta();
        assert(p != null);
        long txnDelta = p.getSecond();
        
        if (debug.val)
            LOG.debug(String.format("CURRENT %d / LAST %d", txnDelta, this.lastDelta));
        
        if (this.lastDelta == 0 && txnDelta == 0) {
            int pollIndex = results.getCompletedIntervalCount();
            String error = String.format("The results at poll interval %d are zero. Halting benchmark...", pollIndex);
            LOG.error(error);
            this.notifyObservers(error);
        }
        this.lastDelta = txnDelta;
    }

    @Override
    public void markEvictionStart() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void markEvictionStop() {
        // TODO Auto-generated method stub
        
    }
}
