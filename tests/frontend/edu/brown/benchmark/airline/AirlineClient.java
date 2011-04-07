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
import java.util.*;

import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ClientResponse;
import org.voltdb.VoltTable;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.benchmark.*;

import edu.brown.benchmark.airline.procedures.*;
import edu.brown.benchmark.airline.util.CustomerId;
import edu.brown.benchmark.airline.util.FlightId;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public class AirlineClient extends AirlineBaseClient {

    /**
     * Airline Benchmark Transactions
     */
    public static enum Transaction {
        CHANGE_SEAT                     (AirlineConstants.FREQUENCY_CHANGE_SEAT),
        FIND_FLIGHT_BY_AIRPORT          (AirlineConstants.FREQUENCY_FIND_FLIGHT_BY_AIRPORT),
        FIND_FLIGHT_BY_NEARBY_AIRPORT   (AirlineConstants.FREQUENCY_FIND_FLIGHT_BY_NEARBY_AIRPORT),
        FIND_OPEN_SEATS                 (AirlineConstants.FREQUENCY_FIND_OPEN_SEATS),
        NEW_RESERVATION                 (AirlineConstants.FREQUENCY_NEW_RESERVATION),
        UPDATE_FREQUENT_FLYER           (AirlineConstants.FREQUENCY_UPDATE_FREQUENT_FLYER),
        UPDATE_RESERVATION              (AirlineConstants.FREQUENCY_UPDATE_RESERVATION);
        
        private Transaction(int weight) {
            this.default_weight = weight;
            this.displayName = StringUtil.title(this.name().replace("_", " "));
        }

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
    // REQUIRED DATA MEMBERS
    // -----------------------------------------------------------------

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = AirlineProjectBuilder.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends ClientMain> m_loaderClass = AirlineLoader.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final String m_jarFileName = "airline.jar";    
    
    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final Map<Transaction, Integer> weights = new HashMap<Transaction, Integer>();
    private final Transaction xacts[] = new Transaction[100];
    
    private final RandomDistribution.FlatHistogram airport_rand;
    
    /**
     * When a customer looks for an open seat, they will then attempt to book that seat in
     * a new reservation. Some of them will want to change their seats. This data structure
     * represents a customer that is queued to change their seat. 
     */
    private class Reservation {
        public final FlightId flight_id;
        public final CustomerId customer_id;
        public final long seatnum;
        
        public Reservation(FlightId flight_id, CustomerId customer_id, long seatnum) {
            this.flight_id = flight_id;
            this.customer_id = customer_id;
            this.seatnum = seatnum;
        }
    } // END CLASS
    
    private final Queue<Reservation> pending_reservations = new LinkedList<Reservation>();
    private final Queue<Reservation> pending_seatchanges = new LinkedList<Reservation>();
    
    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public static void main(String args[]) {
        org.voltdb.benchmark.ClientMain.main(AirlineClient.class, args, false);
    }

    public AirlineClient(String[] args) {
        super(args);

        // Initialize Default Weights
        for (Transaction t : Transaction.values()) {
            this.weights.put(t, t.getDefaultWeight());
        } // FOR

        // Process additional parameters
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);
            
            // Transaction Weights
            // Expected format: -Dxactweight=TRANSACTION_NAME:###
            if (key.equals("xactweight")) {
                String parts[] = value.split(":");
                Transaction t = Transaction.get(parts[0]);
                assert(t == null) : "Invalid transaction name '" + parts[0] + "'";
                Integer weight = Integer.parseInt(parts[1]);
                assert(weight == null) : "Invalid weight '" + parts[1] + "' for transaction " + t;
                this.weights.put(t, weight);
            }
        } // FOR
        
        // Make sure we have the information we need in the BenchmarkProfile
        String error_msg = null;
        if (this.m_profile.getFlightIdCount() == 0) {
            error_msg = "The benchmark profile does not have any flight ids.";
        } else if (this.m_profile.getCustomerIdCount() == 0) {
            error_msg = "The benchmark profile does not have any customer ids.";
        } else if (this.m_profile.getFlightStartDate() == null) {
            error_msg = "The benchmark profile does not have a valid flight start date.";
        }
        if (error_msg != null) {
            LOG.error(error_msg + " Unable to start client.");
            System.exit(1);
        }
        
        // Create xact lookup array
        int total = 0;
        for (Transaction t : Transaction.values()) {
            for (int i = 0, cnt = this.weights.get(t); i < cnt; i++) {
                this.xacts[total++] = t;
            } // FOR
        } // FOR
        assert(total == xacts.length) : "The total weight for the transactions is " + total + ". It needs to be " + xacts.length;
        
        //
        // Load Histograms
        //
        LOG.info("Loading data files for histograms");
        this.loadHistograms();
        
        Histogram<String> histogram = this.getHistogram(AirlineConstants.HISTOGRAM_POPULATION_PER_AIRPORT);
        this.airport_rand = new RandomDistribution.FlatHistogram<String>(m_rng, histogram);
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
        //
        // Execute Transactions
        //
        try {
            while (true) {
                executeTransaction();
                m_voltClient.backpressureBarrier();
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

    
    /**
     * 
     * @return
     * @throws IOException
     */
    private void executeTransaction() throws IOException {
        int idx = this.m_rng.number(0, this.xacts.length);
        assert(idx >= 0);
        assert(idx < this.xacts.length);
        
        LOG.info("Executing new invocation of transaction " + this.xacts[idx]);
        switch (this.xacts[idx]) {
            case CHANGE_SEAT: {
                if (!this.pending_seatchanges.isEmpty()) this.executeChangeSeat();
                break;
            }
            case FIND_FLIGHT_BY_AIRPORT: {
                this.executeFindFlightByAirport();
                break;
            }
            case FIND_FLIGHT_BY_NEARBY_AIRPORT: {
                break;
            }
            case FIND_OPEN_SEATS: {
                this.runFindOpenSeats();
                break;
            }
            case NEW_RESERVATION: {
                if (!this.pending_reservations.isEmpty()) this.executeNewReservation();
                break;
            }
            case UPDATE_FREQUENT_FLYER: {
                this.executeUpdateFrequentFlyer();
                break;
            }
            case UPDATE_RESERVATION: {
                this.executeUpdateReservation();
                break;
            }
            default:
                assert(false) : "Unexpected transaction: " + this.xacts[idx]; 
        } // SWITCH
        return;
    }
    
    // -----------------------------------------------------------------
    // ChangeSeat
    // -----------------------------------------------------------------
    
    private class ChangeSeatCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_counts[Transaction.CHANGE_SEAT.ordinal()].incrementAndGet();
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                assert (clientResponse.getResults().length == 1);
                assert (clientResponse.getResults()[0].getRowCount() == 1);
                assert (clientResponse.getResults()[0].asScalarLong() == 1 ||
                        clientResponse.getResults()[0].asScalarLong() == 0);
            }
        }
    }

    private void executeChangeSeat() throws IOException {
        // Pull off the first pending seat change and throw that ma at the server
        Reservation r = this.pending_seatchanges.remove();
        m_voltClient.callProcedure(new ChangeSeatCallback(), ChangeSeat.class.getSimpleName(), 
                                   r.flight_id.encode(), r.customer_id.encode(), r.seatnum);
    }
    
    // ----------------------------------------------------------------
    // NewReservation
    // ----------------------------------------------------------------
    
    class NewReservationCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_counts[Transaction.NEW_RESERVATION.ordinal()].incrementAndGet();
            VoltTable[] results = clientResponse.getResults();
            assert (results.length == 1);
            assert (results[0].getRowCount() == 1);
            assert (results[0].asScalarLong() == 1);
        }
    }

    private void executeNewReservation() throws IOException {
        Reservation r = this.pending_reservations.remove();
        long r_id = this.m_profile.getRecordCount(AirlineConstants.TABLENAME_RESERVATION);
        
        // Generate random attributes
        long attributes[] = new long[9];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = m_rng.nextLong();
        } // FOR

        m_voltClient.callProcedure(new NewReservationCallback(),
                                   NewReservation.class.getSimpleName(),
                                   r_id, r.flight_id.encode(), r.customer_id.encode(), r.seatnum, attributes);
    }
    

    // ----------------------------------------------------------------
    // UpdateFrequentFlyer
    // ----------------------------------------------------------------
    
    class UpdateFrequentFlyerCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_counts[Transaction.UPDATE_FREQUENT_FLYER.ordinal()].incrementAndGet();
            VoltTable[] results = clientResponse.getResults();
            assert (results.length == 1);
            assert (results[0].getRowCount() == 1);
            assert (results[0].asScalarLong() == 1);
        }
    }

    private void executeUpdateFrequentFlyer() throws IOException {
        // Pick a random customer and then have at it!
        CustomerId customer_id = this.getRandomCustomerId(this.airport_rand.nextInt());
        long airline_id = this.getRandomAirlineId();
        long attr0 = this.m_rng.nextLong();
        long attr1 = this.m_rng.nextLong();

        m_voltClient.callProcedure(new UpdateFrequentFlyerCallback(),
                                   UpdateFrequentFlyer.class.getSimpleName(),
                                   customer_id.encode(), airline_id, attr0, attr1);
    }

    // ----------------------------------------------------------------
    // UpdateReservation
    // ----------------------------------------------------------------
    
    class UpdateReservationCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_counts[Transaction.UPDATE_RESERVATION.ordinal()].incrementAndGet();
            VoltTable[] results = clientResponse.getResults();
            assert (results.length == 1);
            assert (results[0].getRowCount() == 1);
            assert (results[0].asScalarLong() == 1);
        }
    }

    private void executeUpdateReservation() throws IOException {
        // Pick a random reservation id
        long r_id = this.m_rng.number(this.m_profile.getReservationUpcomingOffset(), this.m_profile.getRecordCount(AirlineConstants.TABLENAME_RESERVATION));
        long value = m_rng.number(1, 1 << 20);
        long attribute_idx = m_rng.number(0, UpdateReservation.NUM_UPDATES);

        m_voltClient.callProcedure(new UpdateReservationCallback(),
                                   UpdateReservation.class.getSimpleName(),
                                   r_id, value, attribute_idx);
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------
    
    class FindOpenSeatsCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_counts[Transaction.FIND_OPEN_SEATS.ordinal()].incrementAndGet();
            VoltTable[] results = clientResponse.getResults();
            assert (results.length == 1);
            assert (results[0].getRowCount() < 150);
            // there is some tiny probability of an empty flight .. maybe
            // 1/(20**150)
            // if you hit this assert (with valid code), play the lottery!
            assert (results[0].getRowCount() > 1);
            
            // Store the pending reservation in our queue for a later transaction by using
            // the first empty seat. With a certain probability, the customer will later ask
            // to change their seat reservation, so we need to queue that up
            int cnt = (m_rng.nextInt(100) < AirlineConstants.PROB_CHANGE_SEAT ? 2 : 1);
            for (int i = 0; i < cnt; i++) {
                results[0].advanceRow();
                
                FlightId flight_id = new FlightId(results[0].getLong(0));
                long seatnum = results[0].getLong(1);
                long airport_depart_id = flight_id.getDepartAirportId();
                CustomerId customer_id = AirlineClient.this.getRandomCustomerId(airport_depart_id);
                
                Reservation r = new Reservation(flight_id, customer_id, seatnum);
                if (i == 0) {
                    AirlineClient.this.pending_reservations.add(r);
                } else if (i == 1) {
                    AirlineClient.this.pending_seatchanges.add(r);
                } else {
                    assert(false);
                }
            } // FOR
        }

    }

    /**
     * Execute the FindOpenSeat procedure
     * @throws IOException
     */
    private void runFindOpenSeats() throws IOException {
        FlightId flight_id = this.getRandomFlightId();
        assert(flight_id != null);
        m_voltClient.callProcedure(new FindOpenSeatsCallback(), FindOpenSeats.class.getSimpleName(), flight_id.encode());
    }
    
    // ----------------------------------------------------------------
    // FindFlightByAirport
    // ----------------------------------------------------------------
    
    class FindFlightByAirportCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_counts[Transaction.FIND_OPEN_SEATS.ordinal()].incrementAndGet();
            VoltTable[] results = clientResponse.getResults();
            assert (results.length == 1);
            assert (results[0].getRowCount() < 150);
            // there is some tiny probability of an empty flight .. maybe
            // 1/(20**150)
            // if you hit this assert (with valid code), play the lottery!
            assert (results[0].getRowCount() > 1);
        }

    }

    private void executeFindFlightByAirport() throws IOException {
        //
        // Select two random airport ids
        // Does it matter whether the one airport actually flies to the other one?
        //
        long depart_airport_id = this.getRandomAirportId();
        long arrive_airport_id;
        do {
            arrive_airport_id = this.getRandomAirportId();
        } while (arrive_airport_id == depart_airport_id);
        
        //
        // Select a random date from our upcoming dates
        //
        Date start_date = this.getRandomUpcomingDate();
        Date stop_date = new Date(start_date.getTime() + AirlineConstants.MILISECONDS_PER_DAY);
        
        m_voltClient.callProcedure(new FindFlightByAirportCallback(),
                                   FindFlightByAirportCallback.class.getSimpleName(),
                                   depart_airport_id, arrive_airport_id, start_date, stop_date);
    }
    
    @Override
    public String getApplicationName() {
        return "Airline Benchmark";
    }

    @Override
    public String getSubApplicationName() {
        return "Client";
    }
}
