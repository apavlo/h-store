/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.client;

import java.io.IOException;
import java.net.UnknownHostException;

import org.voltdb.StoredProcedureInvocationHints;
import org.voltdb.VoltTable;

import edu.brown.profilers.ProfileMeasurement;

/**
 *  <p>
 *  A <code>Client</code> that connects to one or more nodes in a volt cluster
 *  and provides methods for invoking stored procedures and receiving
 *  responses. Client applications that are resource constrained (memory, CPU) or high performance
 *  (process hundreds of thousands of transactions per second) will want to pay attention to the hints that
 *  can be provided via
 *  {@link ClientFactory#createClient(int, int[], boolean, StatsUploaderSettings)} and
 *  {@link #callProcedure(ProcedureCallback, int, String, Object...)}.
 *  Most Client applications will not need to generate enough load for these optimizations to matter.
 *  </p>
 *
 *  <p>
 *  A stored procedure invocation contains approximately 24 bytes of data per
 *  invocation plus the serialized sized of the procedure name and any parameters. The size of any specific procedure
 *  and parameter set can be calculated using
 *  {@link #calculateInvocationSerializedSize(String, Object...) calculateInvocationSerializedSize}. This method
 *  serializes the invocation and returns the serialized size and is computationally intensive and not suitable
 *  for use on a per invocation basis.
 *  </p>
 *
 *  <p>
 *  The expected serialized message size parameter provided to
 *  {@link ClientFactory#createClient(int, int[], boolean, StatsUploaderSettings) createClient}
 *  determines the default size of the initial memory allocation used when serializing stored procedure invocations.
 *  If the initial allocation is insufficient then the allocation is doubled necessitating an extra allocation and copy
 *  If the allocation is excessively small it may be doubled and copied several times when serializing large
 *  parameter sets.
 *  </p>
 *
 *  <p>
 *  {@link #callProcedure(ProcedureCallback, int, String, Object...)} allows the initial allocation to be hinted on a
 *  per procedure invocation basis. This is useful when an application invokes procedures where the size can vary
 *  significantly between invocations or procedures.
 *  </p>
 *
 *  <p>
 *  The <code>Client</code> performs aggressive memory pooling of {@link java.nio.DirectByteBuffer DirectByteBuffers}. The pool
 *  contains arenas with buffers that are sized to powers of 2 from 2^4 to 2^18. Java does not reliably
 *  garbage collect {@link java.nio.DirectByteBuffer DirectByteBuffers} so the pool never returns a buffer to the
 *  heap. The <code>maxArenaSizes</code> array passed to
 *  {@link ClientFactory#createClient(int, int[], boolean, StatsUploaderSettings)} can be used
 *  to specify the maximum size each arena will be allowed to grow to. The client will continue to function even
 *  if the an arena is no longer able to grow. It will fall back to using slower
 *  {@link java.nio.HeapByteBuffer HeapByteBuffers} for serializing invocations.
 *  </p>
 */
public interface Client {

    /**
     * Create a connection to another VoltDB node.
     * @param host hostname or IP address of the host to connect to
     * @param port the port number that the host is listening on
     * @throws UnknownHostException
     * @throws IOException
     */
    public void createConnection(String host, int port) throws UnknownHostException, IOException;
    
    /**
     * Create a connection to another VoltDB node.
     * @param siteId TODO
     * @param host hostname or IP address of the host to connect to
     * @param port TODO
     * @param username Username to authorize. Username is ignored if authentication is disabled.
     * @param password Password to authenticate. Password is ignored if authentication is disabled.
     * @throws UnknownHostException
     * @throws IOException
     */
    public void createConnection(Integer siteId, String host, int port, String username, String password)
        throws UnknownHostException, IOException;
    
    public ClientResponse callStreamProcedure(String procName, Long batchId, Object... parameters)
            throws IOException, NoConnectionsException, ProcCallException;
    
    public ClientResponse callStreamProcedure(String procName, StoredProcedureInvocationHints hints, Long batchId, Object... parameters) 
            throws IOException, NoConnectionsException, ProcCallException;
    

    /**
     * Synchronously invoke a procedure. Blocks until a result is available. A {@link ProcCallException}
     * is thrown if the response is anything other then success.
     * @param procName <code>class</code> name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return array of VoltTable results.
     * @throws org.voltdb.client.ProcCallException
     * @throws NoConnectionsException
     */
    public ClientResponse callProcedure(String procName, Object... parameters)
        throws IOException, NoConnectionsException, ProcCallException;
    
    /**
     * Synchronously invoke a procedure. Blocks until a result is available. A {@link ProcCallException}
     * is thrown if the response is anything other then success.
     * @param procName <code>class</code> name (not qualified by package) of the procedure to execute.
     * @param hints Extra information about what the transaction will do.
     * @param parameters vararg list of procedure's parameter values.
     * @return array of VoltTable results.
     * @throws org.voltdb.client.ProcCallException
     * @throws NoConnectionsException
     */
    public ClientResponse callProcedure(String procName, StoredProcedureInvocationHints hints, Object... parameters)
        throws IOException, NoConnectionsException, ProcCallException;

    /**
     * Asynchronously invoke a procedure. Does not guarantee that the invocation is actually queued. If there
     * is backpressure on all connections to the cluster then the invocation will not be queued. Check the return value
     * to determine if queuing actually took place.
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise
     */
    public boolean callProcedure(ProcedureCallback callback, String procName, Object... parameters)
    throws IOException, NoConnectionsException;

    public boolean callStreamProcedure(ProcedureCallback callback, String procName, Long batchId, Object... parameters)
            throws IOException, NoConnectionsException;
    
    
    /**
     * Asynchronously invoke a procedure. Does not guarantee that the invocation is actually queued. If there
     * is backpressure on all connections to the cluster then the invocation will not be queued. Check the return value
     * to determine if queuing actually took place.
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param hints Extra information about what the transaction will do.
     * @param parameters vararg list of procedure's parameter values.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise
     */
    public boolean callProcedure(ProcedureCallback callback, String procName, StoredProcedureInvocationHints hints, Object... parameters)
    throws IOException, NoConnectionsException;

    public boolean callStreamProcedure(ProcedureCallback callback, String procName, Long batchId, StoredProcedureInvocationHints hints, Object... parameters)
            throws IOException, NoConnectionsException;
    
    public boolean asynCallProcedure(ProcedureCallback callback, String procName, StoredProcedureInvocationHints hints, Object... parameters)
    throws IOException, NoConnectionsException;
    /**
     * Asynchronously invoke a procedure. Does not guarantee that the invocation is actually queued. If there
     * is backpressure on all connections to the cluster then the invocation will not be queued. Check the return value
     * to determine if queuing actually took place. An opportunity is provided to hint what the size of the invocation
     * will be once serialized. This is used to perform more efficient memory allocation and serialization. The size
     * of an invocation can be calculated using {@link #calculateInvocationSerializedSize(String, Object...)}.
     * Only Clients that are resource constrained or expect to process hundreds of thousands of txns/sec will benefit
     * from accurately determining the serialized size of message.
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @param expectedSerializedSize A hint indicating the size the procedure invocation is expected to be
     *                               once serialized. Allocations are done in powers of two.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise
     */
    public boolean callProcedure(
            ProcedureCallback callback,
            int expectedSerializedSize,
            String procName,
            StoredProcedureInvocationHints hints,
            Object... parameters)
    throws IOException, NoConnectionsException;

    public boolean callStreamProcedure(
            ProcedureCallback callback,
            int expectedSerializedSize,
            String procName,
            Long batchId,
            StoredProcedureInvocationHints hints,
            Object... parameters)
    throws IOException, NoConnectionsException;

    
    /**
     * Calculate the size of a stored procedure invocation once it is serialized. This is computationally intensive
     * as the invocation is serialized as part of the calculation.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return The size of the invocation once serialized
     */
    public int calculateInvocationSerializedSize(String procName, Object... parameters);


    /**
     * Block the current thread until all queued stored procedure invocations have received responses
     * or there are no more connections to the cluster
     * @throws NoConnectionsException
     */
    public void drain() throws NoConnectionsException, InterruptedException;

    /**
     * Shutdown the {@link Client} closing all network connections and release all memory resources.
     * Failing to call this method before the {@link Client} is garbage collected can generate errors because
     * <code>finalization</code> is used to detect resource leaks. A client cannot be used once it has
     * been closed.
     * @throws InterruptedException
     */
    public void close() throws InterruptedException;

    /**
     * Add to the list of listeners that will be notified of events
     * @param listener Listener to register
     */
    public void addClientStatusListener(ClientStatusListener listener);

    /**
     * Remove listener so that it will no longer be notified of events
     * @param listener Listener to unregister
     * @return <code>true</code> if the listener was removed and <code>false</code> if it wasn't registered
     */
    public boolean removeClientStatusListener(ClientStatusListener listener);

    /**
     * Blocks the current thread until there is no more backpressure or there are no more connections
     * to the database
     * @throws InterruptedException
     */
    public void backpressureBarrier() throws InterruptedException;

    /**
     * Get IO stats for each connection as well as globally
     * @return Table containing IO stats
     */
    public VoltTable getIOStats();

    /**
     * Get IO stats for each connection as well as globally counting since the last
     * time this method was called. Don't call this interval version
     * if the client API is uploading stats via JDBC, it messes up the counters
     * @return Table containing IO states
     */
    public VoltTable getIOStatsInterval();

    /**
     * Get procedure invocation counts and round trip time stats for each connection
     * @return Table containing procedure stats
     */
    public VoltTable getProcedureStats();

    /**
     * Get procedure invocation counts and round trip time stats for each connection
     * since the last time this method was called. Don't call this interval version
     * if the client API is uploading stats via JDBC, it messes up the counters
     * @return Table containing procedure stats
     */
    public VoltTable getProcedureStatsInterval();

    public ProfileMeasurement getQueueTime();

    /**
     * Get an identifier for the cluster that this client is currently connected to.
     * Will be null if the client has not been connected
     * @return An array of a Long and Integer containing the millisecond timestamp when the cluster was
     * started and the leader address
     */
    public Object[] getInstanceId();

    /**
     * Retrieve the build string that was provided by the server on logon
     * @return Volt server build string
     */
    public String getBuildString();

    /**
     * The default behavior for queuing of asynchronous procedure invocations is to block until
     * it is possible to queue the invocation. If blocking is set to false callProcedure will always return
     * immediately if it is not possible to queue the procedure invocation due to backpressure.
     * @param blocking
     */
    void configureBlocking(boolean blocking);

    /**
     * Whether callProcedure will return immediately if a an async procedure invocation could not be queued
     * due to backpressure
     * @return true if callProcedure will block until backpressure ceases and false otherwise
     */
    boolean blocking();
}
