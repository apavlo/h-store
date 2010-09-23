package edu.brown.benchmark.airline;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.airline.procedures.*;

public class AirlineProjectBuilder extends AbstractProjectBuilder {

    public static final Class<?> PROCEDURES[] = new Class<?>[] {
        FindOpenSeats.class,
        UpdateReservation.class,
        ChangeSeat.class,
    };
    
    public static final String PARTITIONING[][] = 
        new String[][] {
            {AirlineConstants.TABLENAME_CUSTOMER,      "C_ID"},
            {AirlineConstants.TABLENAME_FLIGHT,        "F_ID"},
            {AirlineConstants.TABLENAME_RESERVATION,   "R_F_ID"}
        };

    public AirlineProjectBuilder() {
        super("airline", AirlineProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}
