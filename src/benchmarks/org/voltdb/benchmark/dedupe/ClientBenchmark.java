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

package org.voltdb.benchmark.dedupe;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;

import edu.brown.api.BenchmarkComponent;
import edu.brown.hstore.Hstoreservice.Status;

public class ClientBenchmark extends BenchmarkComponent {
    public static final AtomicLong globalMainId = new AtomicLong(1);
    public static final Random rand = new java.util.Random(1l);

    public static void main(String args[]) {
        edu.brown.api.BenchmarkComponent.main(ClientBenchmark.class, args, false);
    }

    public ClientBenchmark(String[] args) {
        super(args);
    }

    // Retrieved via reflection by BenchmarkController
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = ProjectBuilderX.class;

    // Retrieved via reflection by BenchmarkController
    //public static final Class<? extends ClientMain> m_loaderClass = anyLoader.class;
    public static final Class<? extends BenchmarkComponent> m_loaderClass = null;

    public static final String m_jarFileName = "dedupe.jar";

    class AsyncCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            final Status status = clientResponse.getStatus();
            incrementTransactionCounter(clientResponse, 0);

            if (status != Status.OK) {
                System.err.println("Failed to execute!!!");
                System.err.println(clientResponse.getException());
                System.err.println(clientResponse.getStatusString());
                System.exit(-1);
            } else {
                pClientCallback(clientResponse.getResults());
            }
        }

        protected void pClientCallback(VoltTable[] results) {
        };
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String countDisplayNames[] = new String[1];
        countDisplayNames[0] = "Insert() Calls";
        return countDisplayNames;
    }

    public void doOne() throws IOException {
        // Generate random data

        int max_playerId = 50000000;
        int max_gameId = 2;

        long playerId = rand.nextInt(max_playerId);
        long gameId = rand.nextInt(max_gameId);
        long socialId = 1l;
        long clientId = 1l;
        long visitTimeMillis = System.currentTimeMillis();

        boolean queued = false;

        while (!queued) {
            //long callTime = System.currentTimeMillis();

            queued = this.getClientHandle().callProcedure(new AsyncCallback(), "Insert", playerId, gameId, socialId, clientId, visitTimeMillis, visitTimeMillis);

            if (!queued) {
                try {
                    this.getClientHandle().backpressureBarrier();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        doOne();

        return true;
    }

    @Override
    public void runLoop() {
        try {
            while(true) {
                doOne();
            }
        } catch (NoConnectionsException e) {
            return;
        } catch (Exception e) {
            e.printStackTrace();
            e.printStackTrace(System.err);
            throw new RuntimeException(e);
        }
    }

}

