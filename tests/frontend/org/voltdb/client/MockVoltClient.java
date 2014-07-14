/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.voltdb.ClientResponseDebug;
import org.voltdb.StoredProcedureInvocationHints;
import org.voltdb.VoltTable;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.profilers.ProfileMeasurement;

/** Hack subclass of VoltClient that fakes callProcedure. */
public class MockVoltClient implements Client {
    public MockVoltClient() {
        super();
    }

    public ClientResponse callProcedure(String procName, Object... parameters) throws ProcCallException {
        numCalls += 1;
        calledName = procName;
        calledParameters = parameters;

        if (abortMessage != null) {
            ProcCallException e = new ProcCallException(null ,abortMessage, null);
            abortMessage = null;
            throw e;
        }

        final VoltTable[] result = nextResult;
        if (resetAfterCall) nextResult = null;
        return new ClientResponse() {

            @Override
            public int getClientRoundtrip() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public int getClusterRoundtrip() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public Exception getException() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                return result;
            }
            
            @Override
            public int getResultsSize() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public Status getStatus() {
                return Status.OK;
            }

            @Override
            public byte getAppStatus() {
                return 0;
            }

            @Override
            public String getAppStatusString() {
                return null;
            }
            
            @Override
            public long getClientHandle() {
                // TODO Auto-generated method stub
                return 0;
            }
            @Override
            public long getTransactionId() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public boolean isSinglePartition() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public int getBasePartition() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public int getRestartCounter() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public boolean isInitialized() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public void finish() {
                // TODO Auto-generated method stub
                
            }
            
            @Override
            public boolean isSpeculative() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public boolean hasDebug() {
                // TODO Auto-generated method stub
                return false;
            }

            public ClientResponseDebug getDebug() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public long getInitiateTime() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public void setInitiateTime(long initiateTime) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public long getBatchId() {
                // TODO Auto-generated method stub
                return -1l;
            }
        };
    }

    public String calledName;
    public Object[] calledParameters;
    public VoltTable[] nextResult;
    public int numCalls = 0;
    public boolean resetAfterCall = true;
    public String abortMessage;

    @Override
    public void addClientStatusListener(ClientStatusListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean callProcedure(ProcedureCallback callback, String procName,
            Object... parameters) throws NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void createConnection(String host, int port) throws UnknownHostException, IOException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void createConnection(Integer siteId, String host, int port, String program, String password)
            throws UnknownHostException, IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void drain() throws NoConnectionsException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean removeClientStatusListener(ClientStatusListener listener) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void close() throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public int calculateInvocationSerializedSize(String procName,
            Object... parameters) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean callProcedure(
            ProcedureCallback callback,
            int expectedSerializedSize,
            String procName,
            StoredProcedureInvocationHints hints,
            Object... parameters)
            throws NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void backpressureBarrier() throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public VoltTable getIOStats() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VoltTable getIOStatsInterval() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] getInstanceId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VoltTable getProcedureStats() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VoltTable getProcedureStatsInterval() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getBuildString() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean blocking() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void configureBlocking(boolean blocking) {
        // TODO Auto-generated method stub
    }
    
    @Override
    public ProfileMeasurement getQueueTime() {
        return null;
    }

    @Override
    public ClientResponse callProcedure(String procName, StoredProcedureInvocationHints hints, Object... parameters) throws IOException, NoConnectionsException, ProcCallException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean callProcedure(ProcedureCallback callback, String procName, StoredProcedureInvocationHints hints, Object... parameters) throws IOException, NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean asynCallProcedure(ProcedureCallback callback, String procName, StoredProcedureInvocationHints hints, Object... parameters) throws IOException, NoConnectionsException {
        return callProcedure(callback, procName, hints, parameters);
    }

    @Override
    public ClientResponse callStreamProcedure(String procName, Long batchId, Object... parameters) throws IOException, NoConnectionsException, ProcCallException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClientResponse callStreamProcedure(String procName, StoredProcedureInvocationHints hints, Long batchId, Object... parameters) throws IOException, NoConnectionsException,
            ProcCallException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean callStreamProcedure(ProcedureCallback callback, String procName, Long batchId, Object... parameters) throws IOException, NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean callStreamProcedure(ProcedureCallback callback, String procName, Long batchId, StoredProcedureInvocationHints hints, Object... parameters) throws IOException,
            NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean callStreamProcedure(ProcedureCallback callback, int expectedSerializedSize, String procName, Long batchId, StoredProcedureInvocationHints hints, Object... parameters)
            throws IOException, NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }
}
