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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.seats.procedures.DeleteReservation;
import edu.brown.benchmark.seats.procedures.FindFlights;
import edu.brown.benchmark.seats.procedures.FindOpenSeats;
import edu.brown.benchmark.seats.procedures.NewReservation;
import edu.brown.benchmark.seats.procedures.UpdateCustomer;
import edu.brown.benchmark.seats.procedures.UpdateReservation;
import edu.brown.benchmark.seats.util.CustomerId;
import edu.brown.benchmark.seats.util.ErrorType;
import edu.brown.benchmark.seats.util.FlightId;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public class SEATSClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(SEATSClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Airline Benchmark Transactions
     */
    public static enum Transaction {
        DELETE_RESERVATION          (DeleteReservation.class,   SEATSConstants.FREQUENCY_DELETE_RESERVATION),
        FIND_FLIGHTS                (FindFlights.class,         SEATSConstants.FREQUENCY_FIND_FLIGHTS),
        FIND_OPEN_SEATS             (FindOpenSeats.class,       SEATSConstants.FREQUENCY_FIND_OPEN_SEATS),
        NEW_RESERVATION             (NewReservation.class,      SEATSConstants.FREQUENCY_NEW_RESERVATION),
        UPDATE_CUSTOMER             (UpdateCustomer.class,      SEATSConstants.FREQUENCY_UPDATE_CUSTOMER),
        UPDATE_RESERVATION          (UpdateReservation.class,   SEATSConstants.FREQUENCY_UPDATE_RESERVATION);
        
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
    
    protected final Map<CacheType, LinkedList<Reservation>> CACHE_RESERVATIONS = new HashMap<CacheType, LinkedList<Reservation>>();
    {
        for (CacheType ctype : CacheType.values()) {
            CACHE_RESERVATIONS.put(ctype, new LinkedList<Reservation>());
        } // FOR
    } 
    
    
    protected final Map<CustomerId, Set<FlightId>> CACHE_CUSTOMER_BOOKED_FLIGHTS = new HashMap<CustomerId, Set<FlightId>>();
    protected final Map<FlightId, BitSet> CACHE_BOOKED_SEATS = new HashMap<FlightId, BitSet>();

    private static final BitSet FULL_FLIGHT_BITSET = new BitSet(SEATSConstants.FLIGHTS_NUM_SEATS);
    static {
        for (int i = 0; i < SEATSConstants.FLIGHTS_NUM_SEATS; i++)
            FULL_FLIGHT_BITSET.set(i);
    } // STATIC
    
    protected BitSet getSeatsBitSet(FlightId flight_id) {
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
    protected boolean isCustomerBookedOnFlight(CustomerId customer_id, FlightId flight_id) {
        Set<FlightId> flights = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        return (flights != null && flights.contains(flight_id));
    }
    
    protected Set<FlightId> getCustomerBookedFlights(CustomerId customer_id) {
        Set<FlightId> f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        if (f_ids == null) {
            f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
            if (f_ids == null) {
                f_ids = new HashSet<FlightId>();
                CACHE_CUSTOMER_BOOKED_FLIGHTS.put(customer_id, f_ids);
            }
        }
        return (f_ids);
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
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final SEATSProfile profile;
    private final RandomGenerator rng;
    private final AtomicBoolean first = new AtomicBoolean(true);
    private final RandomDistribution.FlatHistogram<Transaction> xacts;
    
    /**
     * When a customer looks for an open seat, they will then attempt to book that seat in
     * a new reservation. Some of them will want to change their seats. This data structure
     * represents a customer that is queued to change their seat. 
     */
    protected static class Reservation {
        public final long id;
        public final FlightId flight_id;
        public final CustomerId customer_id;
        public final int seatnum;
        
        public Reservation(long id, FlightId flight_id, CustomerId customer_id, int seatnum) {
            this.id = id;
            this.flight_id = flight_id;
            this.customer_id = customer_id;
            this.seatnum = seatnum;
            assert(this.seatnum >= 0) : "Invalid seat number\n" + this;
            assert(this.seatnum < SEATSConstants.FLIGHTS_NUM_SEATS) : "Invalid seat number\n" + this;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Reservation) {
                Reservation r = (Reservation)obj;
                // Ignore id!
                return (this.seatnum == r.seatnum &&
                        this.flight_id.equals(r.flight_id) &&
                        this.customer_id.equals(r.customer_id));
                        
            }
            return (false);
        }
        
        @Override
        public String toString() {
            return String.format("{Id:%d / %s / %s / SeatNum:%d}",
                                 this.id, this.flight_id, this.customer_id, this.seatnum);
        }
    } // END CLASS

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public static void main(String args[]) {
        edu.brown.benchmark.BenchmarkComponent.main(SEATSClient.class, args, false);
    }

    public SEATSClient(String[] args) {
        super(args);

        this.rng = new RandomGenerator(0); // FIXME
        this.profile = new SEATSProfile(this.getCatalog(), this.rng);
    
        if (this.noClientConnections() == false) {
            this.profile.loadProfile(this.getClientHandle());
            if (trace.get()) LOG.trace("Airport Max Customer Id:\n" + this.profile.airport_max_customer_id);
        
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
        final Histogram<Transaction> weights = new Histogram<Transaction>();
        for (Transaction t : Transaction.values()) {
            int weight = this.getTransactionWeight(t.getExecName(), t.getDefaultWeight());
            weights.put(t, weight);
        } // FOR
        
        // Create xact lookup array
        this.xacts = new RandomDistribution.FlatHistogram<Transaction>(rng, weights);
        assert(weights.getSampleCount() == 100) : "The total weight for the transactions is " + this.xacts.getSampleCount() + ". It needs to be 100";
        if (debug.get()) LOG.debug("Transaction Execution Distribution:\n" + weights);
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
            try {
                boolean ret = this.executeFindOpenSeats(Transaction.FIND_OPEN_SEATS);
                assert(ret);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        
        int tries = 10;
        boolean ret = false;
        while (tries-- > 0 && ret == false) {
            Transaction txn = this.xacts.nextValue();
            if (debug.get()) LOG.debug("Attempting to execute " + txn);
            switch (txn) {
                case DELETE_RESERVATION: {
                    ret = this.executeDeleteReservation(txn);
                    break;
                }
                case FIND_FLIGHTS: {
                    ret = this.executeFindFlights(txn);
                    break;
                }
                case FIND_OPEN_SEATS: {
                    ret = this.executeFindOpenSeats(txn);
                    break;
                }
                case NEW_RESERVATION: {
                    ret = this.executeNewReservation(txn);
                    break;
                }
                case UPDATE_CUSTOMER: {
                    ret = this.executeUpdateCustomer(txn);
                    break;
                }
                case UPDATE_RESERVATION: {
                    ret = this.executeUpdateReservation(txn);
                    break;
                }
                default:
                    assert(false) : "Unexpected transaction: " + txn; 
            } // SWITCH
            if (ret && debug.get()) LOG.debug("Executed a new invocation of " + txn);
        }
        if (tries == 0 && debug.get()) LOG.warn("I have nothing to do!");
        return (tries > 0);
    }
    
    /**
     * Take an existing Reservation that we know is legit and randomly decide to 
     * either queue it for a later update or delete transaction 
     * @param r
     */
    protected void requeueReservation(Reservation r) {
        CacheType ctype = null;
        
        // Queue this motha trucka up for a deletin'
        if (rng.nextBoolean()) {
            ctype = CacheType.PENDING_DELETES;
        } else {
            ctype = CacheType.PENDING_UPDATES;
        }
        assert(ctype != null);
        
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(ctype);
        assert(cache != null);
        cache.add(r);
        if (debug.get())
            LOG.debug(String.format("Queued %s for %s [cache=%d]", r, ctype, cache.size()));
        while (cache.size() > ctype.limit) {
            cache.remove();
        } // WHILE
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
                    LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
                    assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
                    synchronized (cache) {
                        cache.add(element);
                    } // SYNCH
                }
            } else if (debug.get()) {
                LOG.info("DeleteReservation " + clientResponse.getStatus() + ": " + clientResponse.getStatusString(), clientResponse.getException());
                LOG.info("BUSTED ID: " + element.flight_id + " / " + element.flight_id.encode());
            }
        }
    }

    private boolean executeDeleteReservation(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        
        // Pull off the first cached reservation and drop it on the cluster...
        Reservation r = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).poll();
        if (r == null) {
            this.stopComputeTime(txn.displayName);
            return (false);
        }
        int rand = rng.number(1, 100);
        
        // Parameters
        long f_id = r.flight_id.encode();
        long c_id = VoltType.NULL_BIGINT;
        String c_id_str = "";
        String ff_c_id_str = "";
        long ff_al_id = VoltType.NULL_BIGINT;
        
        // Delete with the Customer's id as a string 
        if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            c_id_str = Long.toString(r.customer_id.encode());
        }
        // Delete using their FrequentFlyer information
        else if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + SEATSConstants.PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
            ff_c_id_str = Long.toString(r.customer_id.encode());
            ff_al_id = r.flight_id.getAirlineId();
        }
        // Delete using their Customer id
        else {
            c_id = r.customer_id.encode();
        }
        
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        Object params[] = new Object[]{
            f_id,           // [0] f_id
            c_id,           // [1] c_id
            c_id_str,       // [2] c_id_str
            ff_c_id_str,    // [3] ff_c_id_str
            ff_al_id,       // [4] ff_al_id
        };
        
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new DeleteReservationCallback(r),
                                             txn.getExecName(),
                                             params);
        return (true);
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
                int ctr = 0;
                while (results[0].advanceRow()) {
                    FlightId flight_id = new FlightId(results[0].getLong(0));
                    assert(flight_id != null);
                    boolean added = profile.addFlightId(flight_id);
                    if (added) ctr++;
                } // WHILE
                if (debug.get()) LOG.debug(String.format("Added %d out of %d FlightIds to local cache",
                                           ctr, results[0].getRowCount()));
            }
        }
    }

    /**
     * Execute one of the FindFlight transactions
     * @param txn
     * @throws IOException
     */
    private boolean executeFindFlights(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        
        long depart_airport_id;
        long arrive_airport_id;
        TimestampType start_date;
        TimestampType stop_date;
        
        // Select two random airport ids
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_RANDOM_AIRPORTS) {
            // Does it matter whether the one airport actually flies to the other one?
            depart_airport_id = this.profile.getRandomAirportId();
            arrive_airport_id = this.profile.getRandomOtherAirport(depart_airport_id);
            
            // Select a random date from our upcoming dates
            start_date = this.profile.getRandomUpcomingDate();
            stop_date = new TimestampType(start_date.getTime() + (SEATSConstants.MICROSECONDS_PER_DAY * 2));
        }
        
        // Use an existing flight so that we guaranteed to get back results
        else {
            FlightId flight_id = this.profile.getRandomFlightId();
            depart_airport_id = flight_id.getDepartAirportId();
            arrive_airport_id = flight_id.getArriveAirportId();
            
            TimestampType flightDate = flight_id.getDepartDate(this.profile.getFlightStartDate());
            long range = Math.round(SEATSConstants.MICROSECONDS_PER_DAY * 0.5);
            start_date = new TimestampType(flightDate.getTime() - range);
            stop_date = new TimestampType(flightDate.getTime() + range);
            
            if (debug.get())
                LOG.debug(String.format("Using %s as look up in %s: %d / %s",
                                        flight_id, txn, flight_id.encode(), flightDate));
        }
        
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
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new FindFlightsCallback(),
                                             txn.getExecName(),
                                             params);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------

    class FindOpenSeatsCallback extends AbstractCallback<FlightId> {
        final long airport_depart_id;
        final List<Reservation> tmp_reservations = new ArrayList<Reservation>();
        public FindOpenSeatsCallback(FlightId f) {
            super(Transaction.FIND_OPEN_SEATS, f);
            this.airport_depart_id = f.getDepartAirportId(); 
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            VoltTable[] results = clientResponse.getResults();
            if (results.length != 1) {
                if (debug.get()) LOG.warn("Results is " + results.length);
                return;
            }
            int rowCount = results[0].getRowCount();
            assert (rowCount <= SEATSConstants.FLIGHTS_NUM_SEATS) :
                String.format("Unexpected %d open seats returned for %s", rowCount, element);
    
            // there is some tiny probability of an empty flight .. maybe 1/(20**150)
            // if you hit this assert (with valid code), play the lottery!
            if (rowCount == 0) return;
    
            // Store pending reservations in our queue for a later transaction            
            BitSet seats = getSeatsBitSet(element);
            
            while (results[0].advanceRow()) {
                int seatnum = (int)results[0].getLong(1);
              
                // We first try to get a CustomerId based at this departure airport
                if (trace.get())
                    LOG.trace("Looking for a random customer to fly on " + element);
                CustomerId customer_id = profile.getRandomCustomerId(airport_depart_id);
              
                // We will go for a random one if:
                //  (1) The Customer is already booked on this Flight
                //  (2) We already made a new Reservation just now for this Customer
                int tries = SEATSConstants.FLIGHTS_NUM_SEATS;
                while (tries-- > 0 && (customer_id == null)) { //  || isCustomerBookedOnFlight(customer_id, flight_id))) {
                    customer_id = profile.getRandomCustomerId();
                    if (trace.get())
                        LOG.trace("RANDOM CUSTOMER: " + customer_id);
                } // WHILE
                assert(customer_id != null) :
                    String.format("Failed to find a unique Customer to reserve for seat #%d on %s", seatnum, element);
        
                Reservation r = new Reservation(profile.getNextReservationId(getClientId()),
                                                element,
                                                customer_id,
                                                seatnum);
                seats.set(seatnum);
                tmp_reservations.add(r);
                if (trace.get())
                    LOG.trace("QUEUED INSERT: " + element + " / " + element.encode() + " -> " + customer_id);
            } // WHILE
          
            if (tmp_reservations.isEmpty() == false) {
                LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
                assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
                
                Collections.shuffle(tmp_reservations);
                synchronized (cache) {
                    cache.addAll(tmp_reservations);
                    while (cache.size() > SEATSConstants.CACHE_LIMIT_PENDING_INSERTS) {
                        cache.remove();
                    } // WHILE
                } // SYNCH
                if (debug.get())
                    LOG.debug(String.format("Stored %d pending inserts for %s [totalPendingInserts=%d]",
                              tmp_reservations.size(), element, cache.size()));
            }
        }
    }

    /**
     * Execute the FindOpenSeat procedure
     * @throws IOException
     */
    private boolean executeFindOpenSeats(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        FlightId flight_id = profile.getRandomFlightId();
        assert(flight_id != null);
        
        Object params[] = new Object[] {
            flight_id.encode()
        };
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new FindOpenSeatsCallback(flight_id),
                                             txn.execName,
                                             params);
        return (true);
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
            VoltTable[] results = clientResponse.getResults();
            
            BitSet seats = getSeatsBitSet(element.flight_id);
            
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
                
                if (debug.get())
                    LOG.debug(String.format("Client %02d :: NewReservation %s [ErrorType=%s] - %s",
                                       getClientId(), clientResponse.getStatus(), errorType, clientResponse.getStatusString()),
                                       clientResponse.getException());
                switch (errorType) {
                    case NO_MORE_SEATS: {
                        seats.set(0, SEATSConstants.FLIGHTS_NUM_SEATS);
                        if (debug.get())
                            LOG.debug(String.format("FULL FLIGHT: %s", element.flight_id));                        
                        break;
                    }
                    case CUSTOMER_ALREADY_HAS_SEAT: {
                        Set<FlightId> f_ids = getCustomerBookedFlights(element.customer_id);
                        f_ids.add(element.flight_id);
                        if (debug.get())
                            LOG.debug(String.format("ALREADY BOOKED: %s -> %s", element.customer_id, f_ids));
                        break;
                    }
                    case SEAT_ALREADY_RESERVED: {
                        seats.set(element.seatnum);
                        if (debug.get())
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
//                        if (debug.get()) 
                            LOG.warn(msg);
                        break;
                    }
                    default: {
                        if (debug.get()) LOG.debug("BUSTED ID: " + element.flight_id + " / " + element.flight_id.encode());
                    }
                } // SWITCH
            }
        }
    }
    
    private boolean executeNewReservation(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        Reservation reservation = null;
        BitSet seats = null;
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
        
        if (debug.get())
            LOG.debug(String.format("Attempting to get a new pending insert Reservation [totalPendingInserts=%d]",
                                    cache.size()));
        while (reservation == null) {
            Reservation r = null;
            synchronized (cache) {
                r = cache.poll();
            } // SYNCH
            if (r == null) {
                if (debug.get())
                    LOG.warn("Unable to execute " + txn + " - No available reservations to insert");
                break;
            }
            
            seats = getSeatsBitSet(r.flight_id);
            
            if (isFlightFull(seats)) {
                if (debug.get())
                    LOG.debug(String.format("%s is full", r.flight_id));
                continue;
            }
            // PAVLO: Not sure why this is always coming back as reserved? 
//            else if (seats.get(r.seatnum)) {
//                if (debug.get())
//                    LOG.debug(String.format("Seat #%d on %s is already booked", r.seatnum, r.flight_id));
//                continue;
//            }
            else if (isCustomerBookedOnFlight(r.customer_id, r.flight_id)) {
                if (debug.get())
                    LOG.debug(String.format("%s is already booked on %s", r.customer_id, r.flight_id));
                continue;
            }
            reservation = r; 
        } // WHILE
        if (reservation == null) {
            if (debug.get()) LOG.debug("Failed to find a valid pending insert Reservation\n" + this.toString());
            this.stopComputeTime(txn.displayName);
            return (false);
        }
        
        // Generate a random price for now
        double price = 2.0 * rng.number(SEATSConstants.RESERVATION_PRICE_MIN,
                                        SEATSConstants.RESERVATION_PRICE_MAX);
        
        // Generate random attributes
        long attributes[] = new long[9];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = rng.nextLong();
        } // FOR
        
        Object params[] = new Object[] {
            reservation.id,
            reservation.customer_id.encode(),
            reservation.flight_id.encode(),
            reservation.seatnum,
            price,
            attributes
        };
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new NewReservationCallback(reservation),
                                             txn.getExecName(),
                                             params);
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateCustomer
    // ----------------------------------------------------------------
    
    class UpdateCustomerCallback extends AbstractCallback<CustomerId> {
        public UpdateCustomerCallback(CustomerId c) {
            super(Transaction.UPDATE_CUSTOMER, c);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            VoltTable[] results = clientResponse.getResults();
            if (clientResponse.getStatus() == Status.OK) {
                assert (results.length >= 1);
                assert (results[0].getRowCount() == 1);
//                assert (results[0].asScalarLong() == 1);
            } else if (debug.get()) {
                LOG.debug("UpdateCustomer " + ": " + clientResponse.getStatusString(), clientResponse.getException());
            }
        }
    }
        
    private boolean executeUpdateCustomer(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        
        // Pick a random customer and then have at it!
        CustomerId customer_id = profile.getRandomCustomerId();
        
        long c_id = VoltType.NULL_BIGINT;
        String c_id_str = null;
        long attr0 = this.rng.nextLong();
        long attr1 = this.rng.nextLong();
        long update_ff = (this.rng.number(1, 100) <= SEATSConstants.PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);
        
        // Update with the Customer's id as a string 
        if (rng.nextInt(100) < SEATSConstants.PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
            c_id_str = Long.toString(customer_id.encode());
        }
        // Update using their Customer id
        else {
            c_id = customer_id.encode();
        }
        
        Object params[] = new Object[]{
            c_id,
            c_id_str,
            update_ff,
            attr0,
            attr1
        };

        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new UpdateCustomerCallback(customer_id),
                                             txn.getExecName(),
                                             params);
        return (true);
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

    private boolean executeUpdateReservation(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_UPDATES;
        
        if (trace.get())
            LOG.trace("Let's look for a Reservation that we can update");
        
        // Pull off the first pending seat change and throw that ma at the server
        Reservation r = null;
        try {
            r = cache.poll();
        } catch (Throwable ex) {
            // Nothing
        }
        if (r == null) {
            this.stopComputeTime(txn.displayName);
            return (false);
        }
        
        long value = rng.number(1, 1 << 20);
        long attribute_idx = rng.nextInt(UpdateReservation.NUM_UPDATES);
        long seatnum = rng.number(0, SEATSConstants.FLIGHTS_NUM_SEATS-1);

        Object params[] = new Object[] {
            r.id,
            r.flight_id.encode(),
            r.customer_id.encode(),
            seatnum,
            attribute_idx,
            value
        };
        
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new UpdateReservationCallback(r),
                                             txn.getExecName(), 
                                             params);
        return (true);
    }

}