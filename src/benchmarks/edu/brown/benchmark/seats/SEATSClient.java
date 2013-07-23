/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
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
package edu.brown.benchmark.seats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections15.Buffer;
import org.apache.commons.collections15.BufferUtils;
import org.apache.commons.collections15.buffer.CircularFifoBuffer;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.seats.procedures.DeleteReservation;
import edu.brown.benchmark.seats.procedures.FindFlights;
import edu.brown.benchmark.seats.procedures.FindOpenSeats;
import edu.brown.benchmark.seats.procedures.NewReservation;
import edu.brown.benchmark.seats.procedures.UpdateCustomer;
import edu.brown.benchmark.seats.procedures.UpdateReservation;
import edu.brown.benchmark.seats.util.ErrorType;
import edu.brown.benchmark.seats.util.Reservation;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.DefaultRandomGenerator;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.StringUtil;

/**
 * SEATS Benchmark Client Driver
 * @author pavlo
 */
public class SEATSClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(SEATSClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Airline Benchmark Transactions
     */
    public static enum Transaction {
        DELETE_RESERVATION  (DeleteReservation.class,   SEATSConstants.FREQUENCY_DELETE_RESERVATION),
        FIND_FLIGHTS        (FindFlights.class,         SEATSConstants.FREQUENCY_FIND_FLIGHTS),
        FIND_OPEN_SEATS     (FindOpenSeats.class,       SEATSConstants.FREQUENCY_FIND_OPEN_SEATS),
        NEW_RESERVATION     (NewReservation.class,      SEATSConstants.FREQUENCY_NEW_RESERVATION),
        UPDATE_CUSTOMER     (UpdateCustomer.class,      SEATSConstants.FREQUENCY_UPDATE_CUSTOMER),
        UPDATE_RESERVATION  (UpdateReservation.class,   SEATSConstants.FREQUENCY_UPDATE_RESERVATION),
        ;
        
        private Transaction(Class<? extends VoltProcedure> proc_class, int weight) {
            this.proc_class = proc_class;
            this.execName = proc_class.getSimpleName();
            this.default_weight = weight;
            this.displayName = StringUtil.title(this.name().replace("_", " "));
        }

        public final Class<? extends VoltProcedure> proc_class;
        public final int default_weight;
        public final String displayName;
        public final String execName;
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        
        public static Transaction get(Integer idx) {
            assert(idx >= 0);
            return (Transaction.idx_lookup.get(idx));
        }

        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name.toLowerCase().intern()));
        }
        public int getDefaultWeight() {
            return (this.default_weight);
        }
        public String getDisplayName() {
            return (this.displayName);
        }
        public String getExecName() {
            return (this.execName);
        }
    }
    
    protected abstract class AbstractCallback<T> implements ProcedureCallback {
        final Transaction txn;
        final T element;
        public AbstractCallback(Transaction txn, T t) {
            this.txn = txn;
            this.element = t;
        }
        public final void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, txn.ordinal());
            this.clientCallbackImpl(clientResponse);

            if (debug.val) {
                if (txn == Transaction.UPDATE_RESERVATION && clientResponse.getStatus() == Status.ABORT_USER) {
                    LOG.error(String.format("Unexpected Error in %s: %s",
                              this.txn.name(), clientResponse.getStatusString()),
                              clientResponse.getException());
                }
                
                if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                    LOG.error(String.format("Unexpected Error in %s: %s",
                              this.txn.name(), clientResponse.getStatusString()),
                              clientResponse.getException());
                }
            }
            
        }
        public abstract void clientCallbackImpl(ClientResponse clientResponse);
    }
    
    // -----------------------------------------------------------------
    // RESERVED SEAT BITMAPS
    // -----------------------------------------------------------------
    
    public enum CacheType {
        PENDING_INSERTS     (SEATSConstants.CACHE_LIMIT_PENDING_INSERTS),
        PENDING_UPDATES     (SEATSConstants.CACHE_LIMIT_PENDING_UPDATES),
        PENDING_DELETES     (SEATSConstants.CACHE_LIMIT_PENDING_DELETES),
        ;
        
        private CacheType(int limit) {
            this.limit = limit;
        }
        private final int limit;
    }
    
    private static final Map<CacheType, Buffer<Reservation>> CACHE_RESERVATIONS = new EnumMap<CacheType, Buffer<Reservation>>(CacheType.class);
    private static final Map<Long, Set<Long>> CACHE_CUSTOMER_BOOKED_FLIGHTS = new ConcurrentHashMap<Long, Set<Long>>();
    private static final Map<Long, BitSet> CACHE_BOOKED_SEATS = new ConcurrentHashMap<Long, BitSet>();
    private static final BitSet FULL_FLIGHT_BITSET = new BitSet(SEATSConstants.FLIGHTS_NUM_SEATS);
    static {
        FULL_FLIGHT_BITSET.set(0, SEATSConstants.FLIGHTS_NUM_SEATS);
        for (CacheType ctype : CacheType.values()) {
            Buffer<Reservation> buffer = BufferUtils.synchronizedBuffer(new CircularFifoBuffer<Reservation>(ctype.limit));
            CACHE_RESERVATIONS.put(ctype, buffer);
        } // FOR
    } // STATIC
    
    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final SEATSProfile profile;
    private final SEATSConfig config;
    private final AbstractRandomGenerator rng;
    private final AtomicBoolean first = new AtomicBoolean(true);
    private final RandomDistribution.FlatHistogram<Transaction> xacts;
    
    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public static void main(String args[]) {
        edu.brown.api.BenchmarkComponent.main(SEATSClient.class, args, false);
    }

    public SEATSClient(String[] args) {
        super(args);

        this.rng = new DefaultRandomGenerator();
        this.config = SEATSConfig.createConfig(this.getCatalogContext(), m_extraParams);
        this.profile = new SEATSProfile(this.getCatalogContext(), this.rng);
    
        if (this.noClientConnections() == false) {
            this.profile.loadProfile(this.getClientHandle());
            if (trace.val) LOG.trace("Airport Max Customer Id:\n" + this.profile.airport_max_customer_id);
        
            // Make sure we have the information we need in the BenchmarkProfile
            String error_msg = null;
            if (this.profile.getFlightIdCount() == 0) {
                error_msg = "The benchmark profile does not have any flight ids.";
            } else if (this.profile.getCustomerIdCount() == 0) {
                error_msg = "The benchmark profile does not have any customer ids.";
            } else if (this.profile.getFlightStartDate() == null) {
                error_msg = "The benchmark profile does not have a valid flight start date.";
            }
            if (error_msg != null) throw new RuntimeException(error_msg);
        }
        
        // Initialize Default Transaction Weights
        final ObjectHistogram<Transaction> weights = new ObjectHistogram<Transaction>();
        for (Transaction t : Transaction.values()) {
            int weight = this.getTransactionWeight(t.getExecName(), t.getDefaultWeight());
            weights.put(t, weight);
        } // FOR
        
        if (this.isSinglePartitionOnly()) {
            weights.clear();
            weights.put(Transaction.FIND_OPEN_SEATS, 75);
            weights.put(Transaction.UPDATE_CUSTOMER, 25);
        }
        
        // Create xact lookup array
        this.xacts = new RandomDistribution.FlatHistogram<Transaction>(this.rng, weights);
        assert(weights.getSampleCount() == 100) :
            "The total weight for the transactions is " + weights.getSampleCount() + ". It needs to be 100";
        if (debug.val)
            LOG.debug("Transaction Execution Distribution:\n" + weights);
    }
    
    protected SEATSProfile getProfile() {
        return this.profile;
    }
        
    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction t : Transaction.values()) {
            names[ii++] = t.getDisplayName();
        }
        return names;
    }
        
    @Override
    protected void runLoop() throws IOException {
        Client client = this.getClientHandle();
        // Execute Transactions
        try {
            while (true) {
                runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        } catch (NoConnectionsException e) {
            /*
             * Client has no clean mechanism for terminating with the DB.
             */
            return;
        } catch (IOException e) {
            /*
             * At shutdown an IOException is thrown for every connection to
             * the DB that is lost Ignore the exception here in order to not
             * get spammed, but will miss lost connections at runtime
             */
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        if (this.first.compareAndSet(true, false)) {
            // Fire off a FindOpenSeats so that we can prime ourselves
            Pair<Object[], ProcedureCallback> ret = this.getFindOpenSeatsParams();
            assert(ret != null);
            Object params[] = ret.getFirst();
            ProcedureCallback callback = ret.getSecond();
            this.getClientHandle().callProcedure(callback, Transaction.FIND_OPEN_SEATS.getExecName(), params);
        }
        
        int tries = 10;
        Pair<Object[], ProcedureCallback> ret = null;
        Transaction txn = null;
        while (tries-- > 0 && ret == null) {
            txn = this.xacts.nextValue();
            if (debug.val) LOG.debug("Attempting to execute " + txn);
            this.startComputeTime(txn.displayName);
            try {
                switch (txn) {
                    case DELETE_RESERVATION: {
                        ret = this.getDeleteReservationParams();
                        break;
                    }
                    case FIND_FLIGHTS: {
                        ret = this.getFindFlightsParams();
                        break;
                    }
                    case FIND_OPEN_SEATS: {
                        ret = this.getFindOpenSeatsParams();
                        break;
                    }
                    case NEW_RESERVATION: {
                        ret = this.getNewReservationParams();
                        break;
                    }
                    case UPDATE_CUSTOMER: {
                        ret = this.getUpdateCustomerParams();
                        break;
                    }
                    case UPDATE_RESERVATION: {
                        ret = this.getUpdateReservationParams();
                        break;
                    }
                    default:
                        assert(false) : "Unexpected transaction: " + txn; 
                } // SWITCH
            } finally {
                this.stopComputeTime(txn.displayName);
            }
            if (ret != null && debug.val) LOG.debug("Executed a new invocation of " + txn);
        }
        if (ret != null) {
            Object params[] = ret.getFirst();
            ProcedureCallback callback = ret.getSecond();
            this.getClientHandle().callProcedure(callback, txn.getExecName(), params);
        }
        
        if (tries == 0 && debug.val) LOG.warn("I have nothing to do!");
        return (tries > 0);
    }
    
    // -----------------------------------------------------------------
    // UTILITY METHODS
    // -----------------------------------------------------------------
    
    /**
     * Take an existing Reservation that we know is legit and randomly decide to 
     * either queue it for a later update or delete transaction 
     * @param r
     */
    protected void requeueReservation(Reservation r) {
        int idx = rng.nextInt(100);
        if (idx > 20) return;
        
        // Queue this motha trucka up for a deletin' or an updatin'
        CacheType ctype = null;
        if (rng.nextBoolean()) {
            ctype = CacheType.PENDING_DELETES;
        } else {
            ctype = CacheType.PENDING_UPDATES;
        }
        assert(ctype != null);
        
        Buffer<Reservation> cache = CACHE_RESERVATIONS.get(ctype);
        assert(cache != null);
        cache.add(r);
        if (debug.val)
            LOG.debug(String.format("Queued %s for %s [cacheSize=%d]\nFlightId: %d\nCustomerId: %d",
                      r, ctype, cache.size(),
                      r.flight_id, r.customer_id));
    }
    
    /**
     * Returns true if the given BitSet for a Flight has all of its seats reserved 
     * @param seats
     * @return
     */
    protected boolean isFlightFull(BitSet seats) {
        assert(FULL_FLIGHT_BITSET.size() == seats.size());
        return FULL_FLIGHT_BITSET.equals(seats);
    }
    
    /**
     * Returns true if the given Customer already has a reservation booked on the target Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerBookedOnFlight(long customer_id, long flight_id) {
        Set<Long> flights = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        return (flights != null && flights.contains(flight_id));
    }
    
    protected final Set<Long> getCustomerBookedFlights(long customer_id) {
        Set<Long> f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        if (f_ids == null) {
            f_ids = new HashSet<Long>();
            CACHE_CUSTOMER_BOOKED_FLIGHTS.put(customer_id, f_ids);
        }
        return (f_ids);
    }
    
    protected final void clearCache() {
        for (BitSet seats : CACHE_BOOKED_SEATS.values()) {
            seats.clear();
        } // FOR
        for (Buffer<Reservation> queue : CACHE_RESERVATIONS.values()) {
            queue.clear();
        } // FOR
        for (Set<Long> f_ids : CACHE_CUSTOMER_BOOKED_FLIGHTS.values()) {
            synchronized (f_ids) {
                f_ids.clear();
            } // SYNCH
        } // FOR
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
            m.put(ctype.name(), CACHE_RESERVATIONS.get(ctype).size());
        } // FOR
        m.put("CACHE_CUSTOMER_BOOKED_FLIGHTS", CACHE_CUSTOMER_BOOKED_FLIGHTS.size()); 
        m.put("CACHE_BOOKED_SEATS", CACHE_BOOKED_SEATS.size());
        m.put("PROFILE", this.profile);
        
        return StringUtil.formatMaps(m);
    }

    // -----------------------------------------------------------------
    // DeleteReservation
    // -----------------------------------------------------------------

    class DeleteReservationCallback extends AbstractCallback<Reservation> {
        public DeleteReservationCallback(Reservation r) {
            super(Transaction.DELETE_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == Status.OK) {
                // We can remove this from our set of full flights because know that there is now a free seat
                BitSet seats = getSeatsBitSet(element.flight_id);
                seats.set(element.seatnum, false);
                
                // And then put it up for a pending insert
                if (rng.nextInt(100) < SEATSConstants.PROB_REQUEUE_DELETED_RESERVATION) {
                    Buffer<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
                    assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
                    synchronized (cache) {
                        cache.add(element);
                    } // SYNCH
                }
            } else if (debug.val) {
                LOG.info("DeleteReservation " + clientResponse.getStatus() + ": " + clientResponse.getStatusString(), clientResponse.getException());
                LOG.info("BUSTED ID: " + element.flight_id + " / " + element.flight_id);
            }
        }
    }
    
    protected Pair<Object[], ProcedureCallback> getDeleteReservationParams() {
        // Pull off the first cached reservation and drop it on the cluster...
        Buffer<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_DELETES;
        Reservation r = null;
        synchronized (cache) {
            if (cache.isEmpty() == false) r = cache.remove();
        } // SYNCH
        if (r == null) {
            return (null);
        }
        int rand;
        if (config.force_all_distributed) {
            rand = SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + 1;
        }
        else if (config.force_all_singlepartition) {
            rand = 100;
        } else {
            rand = rng.number(1, 100);
        }
        
        // Parameters
        long f_id = r.flight_id;
        long c_id = VoltType.NULL_BIGINT;
        String c_id_str = "";
        String ff_c_id_str = "";
        
        // Delete with the Customer's id as a string 
        if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            c_id_str = String.format(SEATSConstants.CUSTOMER_ID_STR, r.customer_id);
        }
        // Delete using their FrequentFlyer information
        else if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + SEATSConstants.PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
            ff_c_id_str = String.format(SEATSConstants.CUSTOMER_ID_STR, r.customer_id);
        }
        // Delete using their Customer id
        else {
            c_id = r.customer_id;
        }
        
        Object params[] = new Object[]{
            f_id,           // [0] f_id
            c_id,           // [1] c_id
            c_id_str,       // [2] c_id_str
            ff_c_id_str,    // [3] ff_c_id_str
        };
        
        if (trace.val) LOG.trace("Calling " + Transaction.DELETE_RESERVATION.getExecName());

        return new Pair<Object[], ProcedureCallback>(params, new DeleteReservationCallback(r));
    }

    // ----------------------------------------------------------------
    // FindFlights
    // ----------------------------------------------------------------
    
    class FindFlightsCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, Transaction.FIND_FLIGHTS.ordinal());
            VoltTable[] results = clientResponse.getResults();
            if (results.length > 1) {
                // Convert the data into a FlightIds that other transactions can use
                while (results[0].advanceRow()) {
                    long flight_id = results[0].getLong(0);
                    assert(flight_id != VoltType.NULL_BIGINT);
                } // WHILE
            }
        }
    }

    /**
     * Execute one of the FindFlight transactions
     * @param txn
     * @throws IOException
     */
    protected Pair<Object[], ProcedureCallback> getFindFlightsParams() {
        // Select two random airport ids
        // Does it matter whether the one airport actually flies to the other one?
        long depart_airport_id = this.profile.getRandomAirportId();
        long arrive_airport_id = this.profile.getRandomOtherAirport(depart_airport_id);
        
        // Select a random date from our upcoming dates
        TimestampType start_date = this.profile.getRandomUpcomingDate();
        TimestampType stop_date = new TimestampType(start_date.getTime() + (SEATSConstants.MICROSECONDS_PER_DAY * 2));
        
        // If distance is greater than zero, then we will also get flights from nearby airports
        long distance = -1;
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
            distance = SEATSConstants.DISTANCES[rng.nextInt(SEATSConstants.DISTANCES.length)];
        }
        
        Object params[] = new Object[] {
            depart_airport_id,
            arrive_airport_id,
            start_date,
            stop_date,
            distance
        };
        if (trace.val) LOG.trace("Calling " + Transaction.FIND_FLIGHTS.getExecName());
        return new Pair<Object[], ProcedureCallback>(params, new FindFlightsCallback());
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------

    class FindOpenSeatsCallback extends AbstractCallback<Long> {
        final List<Reservation> tmp_reservations = new ArrayList<Reservation>();
        public FindOpenSeatsCallback(long f) {
            super(Transaction.FIND_OPEN_SEATS, f);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            // Ignore aborted txns
            if (clientResponse.getStatus() != Status.OK) return;
            
            VoltTable[] results = clientResponse.getResults();
            assert(results.length == 2) :
                String.format("Unexpected result set with %d tables", results.length);

            // FLIGHT INFORMATION
            VoltTable flightInfo = results[0];
            boolean adv = flightInfo.advanceRow();
            assert(adv);
            long flight_id = flightInfo.getLong("F_ID");
            assert(flight_id == element.longValue());

            // OPEN SEAT INFORMATION
            VoltTable seatInfo = results[1];
            int rowCount = seatInfo.getRowCount();
            assert (rowCount <= SEATSConstants.FLIGHTS_NUM_SEATS) :
                String.format("Unexpected %d open seats returned for %s", rowCount, element);
    
            // there is some tiny probability of an empty flight .. maybe 1/(20**150)
            // if you hit this assert (with valid code), play the lottery!
            if (rowCount == 0) return;
    
            // Store pending reservations in our queue for a later transaction            
            BitSet seats = getSeatsBitSet(element);
            
            int clientId = getClientId();
            while (seatInfo.advanceRow()) {
                int seatnum = (int)seatInfo.getLong(1);
                if (seatnum < SEATSConstants.FLIGHTS_RESERVED_SEATS) { 
                    continue;
                }
              
                // Just get a random customer to through on this flight
                if (trace.val)
                    LOG.trace("Looking for a random customer to fly on " + flight_id);
                long customer_id = profile.getRandomCustomerId();
                Reservation r = new Reservation(profile.getNextReservationId(clientId),
                                                flight_id, customer_id, seatnum);
                tmp_reservations.add(r);
                seats.set(seatnum);
                if (trace.val)
                    LOG.trace(String.format("QUEUED INSERT: %s -> %s", flight_id, customer_id));
            } // WHILE
          
            if (tmp_reservations.isEmpty() == false) {
                Buffer<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
                assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
                
                Collections.shuffle(tmp_reservations);
                synchronized (cache) {
                    cache.addAll(tmp_reservations);
                } // SYNCH
                if (debug.val)
                    LOG.debug(String.format("Stored %d pending inserts for %s [totalPendingInserts=%d]",
                              tmp_reservations.size(), flight_id, cache.size()));
            }
        }
    }

    /**
     * Execute the FindOpenSeat procedure
     * @throws IOException
     */
    protected Pair<Object[], ProcedureCallback> getFindOpenSeatsParams() {
        long flight_id = profile.getRandomFlightId();
        Object params[] = new Object[] {
            flight_id
        };
        if (trace.val) LOG.trace("Calling " + Transaction.FIND_OPEN_SEATS.getExecName());
        return new Pair<Object[], ProcedureCallback>(params, new FindOpenSeatsCallback(flight_id));
    }
    
    // ----------------------------------------------------------------
    // NewReservation
    // ----------------------------------------------------------------
    
    class NewReservationCallback extends AbstractCallback<Reservation> {
        public NewReservationCallback(Reservation r) {
            super(Transaction.NEW_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            final VoltTable[] results = clientResponse.getResults();
            final BitSet seats = getSeatsBitSet(element.flight_id);
            
            // Valid NewReservation
            if (clientResponse.getStatus() == Status.OK) {
                assert(results.length > 1);
                assert(results[0].getRowCount() == 1);
                assert(results[0].asScalarLong() == 1);

                // Mark this seat as successfully reserved
                seats.set(element.seatnum);

                // Set it up so we can play with it later
                SEATSClient.this.requeueReservation(element);
            }
            // Aborted - Figure out why!
            else if (clientResponse.getStatus() == Status.ABORT_USER) {
                String msg = clientResponse.getStatusString();
                ErrorType errorType = ErrorType.getErrorType(msg);
                
                if (debug.val)
                    LOG.debug(String.format("Client %02d :: NewReservation %s [ErrorType=%s] - %s",
                              getClientId(), clientResponse.getStatus(), errorType, clientResponse.getStatusString()),
                              clientResponse.getException());
                switch (errorType) {
                    case NO_MORE_SEATS: {
                        seats.set(0, SEATSConstants.FLIGHTS_NUM_SEATS);
                        if (debug.val)
                            LOG.debug(String.format("FULL FLIGHT: %s", element.flight_id));                        
                        break;
                    }
                    case CUSTOMER_ALREADY_HAS_SEAT: {
                        Set<Long> f_ids = getCustomerBookedFlights(element.customer_id);
                        f_ids.add(element.flight_id);
                        if (debug.val)
                            LOG.debug(String.format("ALREADY BOOKED: %s -> %s", element.customer_id, f_ids));
                        break;
                    }
                    case SEAT_ALREADY_RESERVED: {
                        seats.set(element.seatnum);
                        if (debug.val)
                            LOG.debug(String.format("ALREADY BOOKED SEAT: %s/%d -> %s",
                                                    element.customer_id, element.seatnum, seats));
                        break;
                    }
                    case INVALID_CUSTOMER_ID: {
                        LOG.warn("Unexpected invalid CustomerId: " + element.customer_id);
                        break;
                    }
                    case INVALID_FLIGHT_ID: {
                        LOG.warn("Unexpected invalid FlightId: " + element.flight_id);
                        break;
                    }
                    case UNKNOWN: {
//                        if (debug.val) 
                            LOG.warn(msg);
                        break;
                    }
                    default: {
                        if (debug.val) LOG.debug("BUSTED ID: " + element.flight_id + " / " + element.flight_id);
                    }
                } // SWITCH
            }
        }
    }
    
    protected Pair<Object[], ProcedureCallback> getNewReservationParams() {
        Reservation reservation = null;
        BitSet seats = null;
        Buffer<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
        
        if (debug.val)
            LOG.debug(String.format("Attempting to get a new pending insert Reservation [totalPendingInserts=%d]",
                      cache.size()));
        while (reservation == null) {
            Reservation r = null;
            synchronized (cache) {
                if (cache.isEmpty() == false) r = cache.remove();
            } // SYNCH
            if (r == null) {
                if (debug.val)
                    LOG.warn("Unable to execute " + Transaction.DELETE_RESERVATION + " - No available reservations to insert");
                break;
            }
            
            seats = this.getSeatsBitSet(r.flight_id);
            
            if (this.isFlightFull(seats)) {
                if (debug.val)
                    LOG.debug(String.format("%s is full", r.flight_id));
                continue;
            }
            // PAVLO: Not sure why this is always coming back as reserved? 
//            else if (seats.get(r.seatnum)) {
//                if (debug.val)
//                    LOG.debug(String.format("Seat #%d on %s is already booked", r.seatnum, r.flight_id));
//                continue;
//            }
            else if (this.isCustomerBookedOnFlight(r.customer_id, r.flight_id)) {
                if (debug.val)
                    LOG.debug(String.format("%s is already booked on %s", r.customer_id, r.flight_id));
                continue;
            }
            reservation = r; 
        } // WHILE
        if (reservation == null) {
            if (debug.val) LOG.debug("Failed to find a valid pending insert Reservation\n" + this.toString());
            return (null);
        }
        
        // Generate a random price for now
        double price = 2.0 * rng.number(SEATSConstants.RESERVATION_PRICE_MIN,
                                        SEATSConstants.RESERVATION_PRICE_MAX);
        
        // Generate random attributes
        long attributes[] = new long[SEATSConstants.NEW_RESERVATION_ATTRS_SIZE];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = rng.nextLong();
        } // FOR
        
        // boolean updateCustomer = (rng.nextInt(100) < SEATSConstants.PROB_UPDATE_CUSTOMER_NEW_RESERVATION);
        
        Object params[] = new Object[] {
            reservation.id,
            reservation.customer_id,
            reservation.flight_id,
            reservation.seatnum,
            price,
            attributes,
            new TimestampType()
        };
        if (trace.val) LOG.trace("Calling " + Transaction.NEW_RESERVATION.getExecName());
        return new Pair<Object[], ProcedureCallback>(params, new NewReservationCallback(reservation));
    }

    // ----------------------------------------------------------------
    // UpdateCustomer
    // ----------------------------------------------------------------
    
    class UpdateCustomerCallback extends AbstractCallback<Long> {
        public UpdateCustomerCallback(long c) {
            super(Transaction.UPDATE_CUSTOMER, c);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            VoltTable[] results = clientResponse.getResults();
            if (clientResponse.getStatus() == Status.OK) {
                assert (results.length >= 1);
                assert (results[0].getRowCount() == 1);
//                assert (results[0].asScalarLong() == 1);
            } else if (debug.val) {
                LOG.debug("UpdateCustomer " + ": " + clientResponse.getStatusString(), clientResponse.getException());
            }
        }
    }
        
    protected Pair<Object[], ProcedureCallback> getUpdateCustomerParams() {
        // Pick a random customer and then have at it!
        long customer_id = profile.getRandomCustomerId();
        
        long c_id = VoltType.NULL_BIGINT;
        String c_id_str = null;
        long attr0 = this.rng.nextLong();
        long attr1 = this.rng.nextLong();
        long update_ff = (this.rng.number(1, 100) <= SEATSConstants.PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);
        if (config.force_all_distributed) update_ff = 1;
        
        // Update with the Customer's id as a string 
        if (rng.nextInt(100) < SEATSConstants.PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
            c_id_str = String.format(SEATSConstants.CUSTOMER_ID_STR, customer_id);
        }
        // Update using their Customer id
        else {
            c_id = customer_id;
        }
        
        Object params[] = new Object[]{
            c_id,
            c_id_str,
            update_ff,
            attr0,
            attr1
        };

        if (trace.val) LOG.trace("Calling " + Transaction.UPDATE_CUSTOMER.getExecName());
        return new Pair<Object[], ProcedureCallback>(params, new UpdateCustomerCallback(customer_id));
    }

    // ----------------------------------------------------------------
    // UpdateReservation
    // ----------------------------------------------------------------

    class UpdateReservationCallback extends AbstractCallback<Reservation> {
        public UpdateReservationCallback(Reservation r) {
            super(Transaction.UPDATE_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == Status.OK) {
                assert (clientResponse.getResults().length == 1);
                assert (clientResponse.getResults()[0].getRowCount() == 1);
                assert (clientResponse.getResults()[0].asScalarLong() == 1 ||
                        clientResponse.getResults()[0].asScalarLong() == 0);
                
                SEATSClient.this.requeueReservation(element);
            }
        }
    }

    protected Pair<Object[], ProcedureCallback> getUpdateReservationParams() {
        if (trace.val)
            LOG.trace("Let's look for a Reservation that we can update");
        
        // Pull off the first pending seat change and throw that ma at the server
        Buffer<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_UPDATES;
        Reservation r = null;
        synchronized (cache) {
            if (cache.isEmpty() == false) r = cache.remove();
        } // SYNCH
        if (r == null) return (null);
        
        long value = rng.number(1, 1 << 20);
        long attribute_idx = rng.nextInt(UpdateReservation.NUM_UPDATES);
        long seatnum = rng.nextInt(SEATSConstants.FLIGHTS_RESERVED_SEATS);
        if (debug.val)
            LOG.debug(String.format("UpdateReservation: FlightId:%d / CustomerId:%d / SeatNum:%d",
                      r.flight_id, r.customer_id, seatnum)); 

        Object params[] = new Object[] {
            r.id,
            r.customer_id,
            r.flight_id,
            seatnum,
            attribute_idx,
            value
        };
        
        if (trace.val) LOG.trace("Calling " + Transaction.UPDATE_RESERVATION.getExecName());
        return new Pair<Object[], ProcedureCallback>(params, new UpdateReservationCallback(r));
    }
    
    protected BitSet getSeatsBitSet(long flight_id) {
        BitSet seats = CACHE_BOOKED_SEATS.get(flight_id);
        if (seats == null) {
//            synchronized (CACHE_BOOKED_SEATS) {
                seats = CACHE_BOOKED_SEATS.get(flight_id);
                if (seats == null) {
                    seats = new BitSet(SEATSConstants.FLIGHTS_NUM_SEATS);
                    CACHE_BOOKED_SEATS.put(flight_id, seats);
                }
//            } // SYNCH
        }
        return (seats);
    }
    

}