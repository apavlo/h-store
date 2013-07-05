package edu.brown.benchmark.seats.util;

import org.voltdb.VoltType;

import edu.brown.benchmark.seats.SEATSConstants;

/**
 * When a customer looks for an open seat, they will then attempt to book that seat in
 * a new reservation. Some of them will want to change their seats. This data structure
 * represents a customer that is queued to change their seat. 
 */
public class Reservation {
    public final long id;
    public final long flight_id;
    public final long customer_id;
    public final int seatnum;
    
    public Reservation(long id, long flight_id, long customer_id, int seatnum) {
        this.id = id;
        this.flight_id = flight_id;
        this.customer_id = customer_id;
        this.seatnum = seatnum;
        
        assert(this.id != VoltType.NULL_BIGINT) : "Null reservation id\n" + this;
        assert(this.seatnum >= 0) : "Invalid seat number\n" + this;
        assert(this.seatnum < SEATSConstants.FLIGHTS_NUM_SEATS) : "Invalid seat number\n" + this;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Reservation) {
            Reservation r = (Reservation)obj;
            // Ignore id!
            return (this.seatnum == r.seatnum &&
                    this.flight_id == r.flight_id &&
                    this.customer_id == r.customer_id);
                    
        }
        return (false);
    }
    
    @Override
    public String toString() {
        return String.format("{Id:%d / %s / %s / SeatNum:%d}",
                             this.id, this.flight_id, this.customer_id, this.seatnum);
    }
} // END CLASS