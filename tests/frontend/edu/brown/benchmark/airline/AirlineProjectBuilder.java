package edu.brown.benchmark.airline;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.airline.procedures.*;

public class AirlineProjectBuilder extends AbstractProjectBuilder {

    public static final Class<?> PROCEDURES[] = new Class<?>[] {
        ChangeSeat.class,
        FindFlightByAirport.class,
        FindFlightByNearbyAirport.class,
        FindOpenSeats.class,
        NewReservation.class,
        UpdateFrequentFlyer.class,
        UpdateReservation.class,
    };
    
    // Transaction Frequencies
    {
        addTransactionFrequency(ChangeSeat.class, AirlineConstants.FREQUENCY_CHANGE_SEAT);
        addTransactionFrequency(FindFlightByAirport.class, AirlineConstants.FREQUENCY_FIND_FLIGHT_BY_AIRPORT);
        addTransactionFrequency(FindFlightByNearbyAirport.class, AirlineConstants.FREQUENCY_FIND_FLIGHT_BY_NEARBY_AIRPORT);
        addTransactionFrequency(FindOpenSeats.class, AirlineConstants.FREQUENCY_FIND_OPEN_SEATS);
        addTransactionFrequency(NewReservation.class, AirlineConstants.FREQUENCY_NEW_RESERVATION);
        addTransactionFrequency(UpdateFrequentFlyer.class, AirlineConstants.FREQUENCY_UPDATE_FREQUENT_FLYER);
        addTransactionFrequency(UpdateReservation.class, AirlineConstants.FREQUENCY_UPDATE_RESERVATION);
    }
    
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
