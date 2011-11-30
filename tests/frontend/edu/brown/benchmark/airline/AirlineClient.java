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

package edu.brown.benchmark.airline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.airline.procedures.*;
import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.hstore.Hstore;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public class AirlineClient extends AirlineBaseClient {
    private static final Logger LOG = Logger.getLogger(AirlineClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Airline Benchmark Transactions
     */
    public static enum Transaction {
        DELETE_RESERVATION          (DeleteReservation.class,   AirlineConstants.FREQUENCY_DELETE_RESERVATION),
        FIND_FLIGHTS                (FindFlights.class,         AirlineConstants.FREQUENCY_FIND_FLIGHTS),
        FIND_OPEN_SEATS             (FindOpenSeats.class,       AirlineConstants.FREQUENCY_FIND_OPEN_SEATS),
        NEW_RESERVATION             (NewReservation.class,      AirlineConstants.FREQUENCY_NEW_RESERVATION),
        UPDATE_CUSTOMER             (UpdateCustomer.class,      AirlineConstants.FREQUENCY_UPDATE_CUSTOMER),
        UPDATE_RESERVATION          (UpdateReservation.class,   AirlineConstants.FREQUENCY_UPDATE_RESERVATION);
        
        private Transaction(Class<? extends VoltProcedure> proc_class, int weight) {
            this.proc_class = proc_class;
            this.default_weight = weight;
            this.displayName = StringUtil.title(this.name().replace("_", " "));
        }

        public final Class<? extends VoltProcedure> proc_class;
        public final int default_weight;
        public final String displayName;
        
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
    }
    
    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final RandomDistribution.FlatHistogram<Transaction> xacts;
    
    /**
     * When a customer looks for an open seat, they will then attempt to book that seat in
     * a new reservation. Some of them will want to change their seats. This data structure
     * represents a customer that is queued to change their seat. 
     */
    private class Reservation {
        public final long id;
        public final FlightId flight_id;
        public final CustomerId customer_id;
        public final long seatnum;
        
        public Reservation(long id, FlightId flight_id, CustomerId customer_id, long seatnum) {
            this.id = id;
            this.flight_id = flight_id;
            this.customer_id = customer_id;
            this.seatnum = seatnum;
        }
    } // END CLASS

    // Shared Cache
    private static final Queue<Reservation> pending_inserts = new LinkedBlockingDeque<Reservation>();
    private static final Queue<Reservation> pending_updates = new LinkedBlockingDeque<Reservation>();
    private static final Queue<Reservation> pending_deletes = new LinkedBlockingDeque<Reservation>();
    
    private static final Set<FlightId> full_flights = new HashSet<FlightId>();
    private static final Map<CustomerId, Set<FlightId>> booked_flights = new HashMap<CustomerId, Set<FlightId>>();
    private static final Map<FlightId, Set<Integer>> booked_seats = new HashMap<FlightId, Set<Integer>>();
    
    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public static void main(String args[]) {
        edu.brown.benchmark.BenchmarkComponent.main(AirlineClient.class, args, false);
    }

    public AirlineClient(String[] args) {
        super(args);

        // Initialize Default Weights
        final Histogram<Transaction> weights = new Histogram<Transaction>();
        for (Transaction t : Transaction.values()) {
            weights.put(t, t.getDefaultWeight());
        } // FOR

        // Process additional parameters
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);
            
            // Transaction Weights
            // Expected format: -Dxactweight=TRANSACTION_NAME:###
            if (key.equalsIgnoreCase("xactweight")) {
                String parts[] = value.split(":");
                Transaction t = Transaction.get(parts[0]);
                assert(t == null) : "Invalid transaction name '" + parts[0] + "'";
                Integer weight = Integer.parseInt(parts[1]);
                assert(weight == null) : "Invalid weight '" + parts[1] + "' for transaction " + t;
                weights.set(t, weight);
            }
        } // FOR
        
        this.profile.loadProfile(this);
        if (debug.get()) LOG.debug("Airport Max Customer Id:\n" + this.profile.airport_max_customer_id);
        
        // Make sure we have the information we need in the BenchmarkProfile
        String error_msg = null;
        if (this.getFlightIdCount() == 0) {
            error_msg = "The benchmark profile does not have any flight ids.";
        } else if (this.getCustomerIdCount() == 0) {
            error_msg = "The benchmark profile does not have any customer ids.";
        } else if (this.getFlightStartDate() == null) {
            error_msg = "The benchmark profile does not have a valid flight start date.";
        }
        if (error_msg != null) throw new RuntimeException(error_msg);
        
        // Create xact lookup array
        this.xacts = new RandomDistribution.FlatHistogram<Transaction>(rng, weights);
        assert(weights.getSampleCount() == 100) : "The total weight for the transactions is " + this.xacts.getSampleCount() + ". It needs to be 100";
        if (debug.get()) LOG.debug("Transaction Execution Distribution:\n" + weights);
        
        // Load Histograms
        if (debug.get()) LOG.debug("Loading data files for histograms");
        this.loadHistograms();
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
    public void runLoop() {
        final Client client = this.getClientHandle();
        
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
        Transaction txn = this.xacts.nextValue();
        if (debug.get()) LOG.info("Executing new invocation of transaction " + txn);
        int tries = 10;
        boolean ret = false;
        while (tries-- > 0 && ret == false) {
            switch (txn) {
                case DELETE_RESERVATION: {
                    if (pending_deletes.isEmpty() == false) ret = this.executeDeleteReservation(txn);
                    break;
                }
                case FIND_FLIGHTS: {
                    ret = this.executeFindFlight(txn);
                    break;
                }
                case FIND_OPEN_SEATS: {
                    ret = this.executeFindOpenSeats(txn);
                    break;
                }
                case NEW_RESERVATION: {
                    if (pending_inserts.isEmpty() == false)
                        ret = this.executeNewReservation(txn);
                    break;
                }
                case UPDATE_CUSTOMER: {
                    ret = this.executeUpdateCustomer(txn);
                    break;
                }
                case UPDATE_RESERVATION: {
                    if (pending_updates.isEmpty() == false) ret = this.executeUpdateReservation(txn);
                    break;
                }
                default:
                    assert(false) : "Unexpected transaction: " + txn; 
            } // SWITCH
        }
        return (tries > 0);
    }
    
    // -----------------------------------------------------------------
    // DeleteReservation
    // -----------------------------------------------------------------
    
    class DeleteReservationCallback implements ProcedureCallback {
        
        final Reservation reservation;
        
        public DeleteReservationCallback(Reservation r) {
            this.reservation = r;
        }
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, Transaction.DELETE_RESERVATION.ordinal());
            if (clientResponse.getStatus() == Hstore.Status.OK) {
                // TODO
            } else if (debug.get()) {
                LOG.info("DeleteReservation " + clientResponse.getStatus() + ": " + clientResponse.getStatusString(), clientResponse.getException());
                LOG.info("BUSTED ID: " + reservation.flight_id + " / " + reservation.flight_id.encode());
            }
        }
    }

    private boolean executeDeleteReservation(Transaction txn) throws IOException {
        // Pull off the first cached reservation and drop it on the cluster...
        Reservation r = pending_deletes.remove();
        assert(r != null);
        int rand = rng.number(1, 100);
        
        Object params[] = new Object[]{
            r.flight_id.encode(),       // [0] f_id
            VoltType.NULL_BIGINT,       // [1] c_id
            "",                         // [2] c_id_str
            "",                         // [3] ff_c_id_str
            VoltType.NULL_BIGINT,       // [4] ff_al_id
        };
        
        // Delete with the Customer's id as a string 
        if (rand <= AirlineConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            params[2] = Long.toString(r.customer_id.encode());
        }
        // Delete using their FrequentFlyer information
        else if (rand <= AirlineConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + AirlineConstants.PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
            params[3] = Long.toString(r.customer_id.encode());
            params[4] = r.flight_id.getAirlineId();
        }
        // Delete using their Customer id
        else {
            params[1] = r.customer_id.encode();
        }
        
        this.getClientHandle().callProcedure(new DeleteReservationCallback(r), txn.proc_class.getSimpleName(), params);
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
                while (results[0].advanceRow()) {
                    FlightId flight_id = new FlightId(results[0].getLong(0));
                    assert(flight_id != null);
                    AirlineClient.this.addFlightId(flight_id);
                } // WHILE
            }
        }
    }

    /**
     * Execute one of the FindFlight transactions
     * @param txn
     * @throws IOException
     */
    private boolean executeFindFlight(Transaction txn) throws IOException {
        long depart_airport_id;
        long arrive_airport_id;
        TimestampType start_date;
        TimestampType stop_date;
        
        // Select two random airport ids
        if (rng.number(1, 100) <= 10) {
            // Does it matter whether the one airport actually flies to the other one?
            depart_airport_id = this.getRandomAirportId();
            arrive_airport_id = this.getRandomOtherAirport(depart_airport_id);
//        long arrive_airport_id = (long)rng.numberExcluding(1, (int)this.getAirportCount(), (int)depart_airport_id);
            
            // Select a random date from our upcoming dates
            start_date = this.getRandomUpcomingDate();
            stop_date = new TimestampType(start_date.getTime() + (AirlineConstants.MICROSECONDS_PER_DAY * 2));
        }
        
        // Use an existing flight so that we guaranteed to get back results
        else {
            FlightId f_id = this.getRandomFlightId();
            depart_airport_id = f_id.getDepartAirportId();
            arrive_airport_id = f_id.getArriveAirportId();
            
            TimestampType flightDate = f_id.getDepartDate(this.getFlightStartDate());
            long range = Math.round(AirlineConstants.MICROSECONDS_PER_DAY * 0.5);
            start_date = new TimestampType(flightDate.getTime() - range);
            stop_date = new TimestampType(flightDate.getTime() + range);
            
            if (debug.get())
                LOG.debug("Using FlightId " + f_id.encode() + " as look up: " + f_id + " / " + flightDate);
        }
        
        // If distance is greater than zero, then we will also get flights from nearby airports
        long distance = -1;
        if (rng.number(1, 100) <= AirlineConstants.PROB_FIND_AIRPORT_NEARBY) {
            distance = AirlineConstants.DISTANCES[rng.nextInt(AirlineConstants.DISTANCES.length)];
        }
        
        Object params[] = new Object[] {
            depart_airport_id,
            arrive_airport_id,
            start_date,
            stop_date,
            distance
        };
        this.getClientHandle().callProcedure(new FindFlightsCallback(), txn.proc_class.getSimpleName(), params);
        return (true);
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------
    
    class FindOpenSeatsCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, Transaction.FIND_OPEN_SEATS.ordinal());
            VoltTable[] results = clientResponse.getResults();
            if (results.length != 1) {
                if (debug.get()) LOG.warn("Results is " + results.length);
                return;
            }
            assert (results[0].getRowCount() < 150);
            // there is some tiny probability of an empty flight .. maybe
            // 1/(20**150)
            // if you hit this assert (with valid code), play the lottery!
            int rowCount = results[0].getRowCount();
            if (rowCount == 0) return;
            
            int insert = (rowCount == 1 ? 1 : rng.nextInt(rowCount-1) + 1);
            Set<Integer> s = rng.getRandomIntSet(insert, rowCount);
            List<Reservation> reservations = new ArrayList<Reservation>();
            for (int i = 0; i < rowCount; i++) {
                // Store pending reservations in our queue for a later transaction
                boolean adv = results[0].advanceRow();
                assert(adv);
                if (s.contains(i) == false) continue;
                
                FlightId flight_id = new FlightId(results[0].getLong(0));
                long seatnum = results[0].getLong(1);
                long airport_depart_id = flight_id.getDepartAirportId();
                CustomerId customer_id = AirlineClient.this.getRandomCustomerId(airport_depart_id);
                if (customer_id == null) {
                    customer_id = AirlineClient.this.getRandomCustomerId();
                    if (debug.get()) LOG.debug("RANDOM CUSTOMER: " + customer_id);
                } else if (debug.get()) {
                    LOG.debug("RANDOM CUSTOMER FOR Airport #" + airport_depart_id + ": " + customer_id);
                }
                assert(customer_id != null);
                
                reservations.add(new Reservation(getNextReservationId(), flight_id, customer_id, seatnum));
                if (debug.get()) LOG.debug("QUEUED INSERT: " + flight_id + " / " + flight_id.encode());
            } // FOR
            if (reservations.isEmpty() == false) {
                Collections.shuffle(reservations, rng);
                AirlineClient.pending_inserts.addAll(reservations);
                while (pending_inserts.size() > 1000) {
                    pending_inserts.remove();
                } // WHILE
            }
        }
    }

    /**
     * Execute the FindOpenSeat procedure
     * @throws IOException
     */
    private boolean executeFindOpenSeats(Transaction txn) throws IOException {
        FlightId flight_id = this.getRandomFlightId();
        assert(flight_id != null);
        this.getClientHandle().callProcedure(new FindOpenSeatsCallback(), txn.proc_class.getSimpleName(), flight_id.encode());
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewReservation
    // ----------------------------------------------------------------
    
    class NewReservationCallback implements ProcedureCallback {
        final Reservation reservation;
        
        public NewReservationCallback(Reservation r) {
            this.reservation = r;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, Transaction.NEW_RESERVATION.ordinal());
            VoltTable[] results = clientResponse.getResults();
            if (clientResponse.getStatus() == Hstore.Status.OK) {
                assert(results.length > 1);
                assert(results[0].getRowCount() == 1);
                assert(results[0].asScalarLong() == 1);

                // Queue this motha trucka up for a deletin'
                if (rng.nextBoolean()) {
                    if (pending_deletes.size() > 1000) pending_deletes.remove();
                    pending_deletes.add(this.reservation);
                }
                // Or queue it for an update
                else if (rng.nextBoolean()) {
                    if (pending_updates.size() > 1000) pending_updates.remove();
                    pending_updates.add(this.reservation);
                }
            } else if (clientResponse.getStatus() == Hstore.Status.ABORT_USER) {
                String msg = clientResponse.getStatusString();
                if (debug.get())
                    LOG.debug(String.format("Client %02d :: NewReservation %s - %s",
                                       getClientId(), clientResponse.getStatus(), clientResponse.getStatusString()),
                                       clientResponse.getException());
                if (msg.startsWith("No more seats available")) {
                    synchronized (full_flights) {
                        full_flights.add(this.reservation.flight_id);
                    } // SYNCH
                    if (debug.get())
                        LOG.debug(String.format("FULL FLIGHT: %s [fullFlights=%d]", this.reservation.flight_id, full_flights.size()));
                }
                else if (msg.contains("already owns on a reservations")) {
                    Set<FlightId> f_ids = null;
                    synchronized (booked_flights) {
                        f_ids = booked_flights.get(this.reservation.customer_id);
                        if (f_ids == null) {
                            f_ids = new HashSet<FlightId>();
                            booked_flights.put(this.reservation.customer_id, f_ids);
                        }
                    } // SYNCH
                    f_ids.add(this.reservation.flight_id);
                    if (debug.get())
                        LOG.debug(String.format("ALREADY BOOKED: %s -> %s", this.reservation.customer_id, f_ids));
                }
                else if (msg.contains("is already reserved")) {
                    Set<Integer> seats = null;
                    synchronized (booked_seats) {
                        seats = booked_seats.get(this.reservation.flight_id);
                        if (seats == null) {
                            seats = new HashSet<Integer>();
                            booked_seats.put(this.reservation.flight_id, seats);
                        }
                    } // SYNCH
                    seats.add((int)this.reservation.seatnum);
                    if (debug.get())
                        LOG.debug(String.format("ALREADY BOOKED SEAT: %s/%d -> %s",
                                           this.reservation.customer_id, this.reservation.seatnum, seats));
                }
                else if (debug.get()) {
                    LOG.debug("BUSTED ID: " + reservation.flight_id + " / " + reservation.flight_id.encode());
                }
            }
        }
    }

    private boolean executeNewReservation(Transaction txn) throws IOException {
        Reservation r = null;
        while (r == null && pending_inserts.isEmpty() == false) {
            Reservation temp = pending_inserts.remove();
            if (full_flights.contains(temp.flight_id)) continue;
            else if (booked_flights.get(temp.customer_id) != null && booked_flights.get(temp.customer_id).contains(temp.flight_id)) continue;
            else if (booked_seats.get(temp.flight_id) != null && booked_seats.get(temp.flight_id).contains(temp.seatnum)) continue;
            r = temp; 
        } // WHILE
        if (r == null) return (false);
        
        // Generate a random price for now
        double price = rng.nextInt(1000) * 2.0;
        
        // Generate random attributes
        long attributes[] = new long[9];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = rng.nextLong();
        } // FOR

        this.getClientHandle().callProcedure(new NewReservationCallback(r),
                                             txn.proc_class.getSimpleName(),
                                             r.id, r.customer_id.encode(), r.flight_id.encode(), r.seatnum, price, attributes);
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateCustomer
    // ----------------------------------------------------------------
    
    class UpdateCustomerCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, Transaction.UPDATE_CUSTOMER.ordinal());
            VoltTable[] results = clientResponse.getResults();
            if (clientResponse.getStatus() == Hstore.Status.OK) {
                assert (results.length >= 1);
                assert (results[0].getRowCount() == 1);
//                assert (results[0].asScalarLong() == 1);
            } else if (debug.get()) {
                LOG.debug("UpdateCustomer " + ": " + clientResponse.getStatusString(), clientResponse.getException());
            }
        }
    }

    private boolean executeUpdateCustomer(Transaction txn) throws IOException {
        // Pick a random customer and then have at it!
        CustomerId customer_id = this.getRandomCustomerId();
        long attr0 = this.rng.nextLong();
        long attr1 = this.rng.nextLong();
        long update_ff = (rng.number(1, 100) <= AirlineConstants.PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);
        
        Object params[] = new Object[]{
            VoltType.NULL_BIGINT,
            "",
            update_ff,
            attr0,
            attr1
        };
        
        // Update with the Customer's id as a string 
        int rand = rng.number(1, 100);
        if (rand <= AirlineConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            params[1] = Long.toString(customer_id.encode());
        }
        // Update using their Customer id
        else {
            params[0] = customer_id.encode();
        }

        this.getClientHandle().callProcedure(new UpdateCustomerCallback(), txn.proc_class.getSimpleName(), params);
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateReservation
    // ----------------------------------------------------------------
    
    class UpdateReservationCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, Transaction.UPDATE_RESERVATION.ordinal());
            if (clientResponse.getStatus() == Hstore.Status.OK) {
                assert (clientResponse.getResults().length == 1);
                assert (clientResponse.getResults()[0].getRowCount() == 1);
                assert (clientResponse.getResults()[0].asScalarLong() == 1 ||
                        clientResponse.getResults()[0].asScalarLong() == 0);
            }
        }
    }

    private boolean executeUpdateReservation(Transaction txn) throws IOException {
        // Pull off the first pending seat change and throw that ma at the server
        Reservation r = pending_updates.remove();
        assert(r != null);
        
        // Pick a random reservation id
        long value = rng.number(1, 1 << 20);
        long attribute_idx = rng.nextInt(UpdateReservation.NUM_UPDATES);

        this.getClientHandle().callProcedure(new UpdateReservationCallback(),
                                             txn.proc_class.getSimpleName(), 
                                             r.id, r.flight_id.encode(), r.customer_id.encode(), r.seatnum, attribute_idx, value);
        return (true);
    }
}