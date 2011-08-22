package edu.brown.benchmark.airline;


import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.airline.procedures.*;

public class AirlineProjectBuilder extends AbstractProjectBuilder {
    
    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends BenchmarkComponent> m_clientClass = AirlineClient.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = AirlineLoader.class;

    public static final Class<?> PROCEDURES[] = new Class<?>[] {
        DeleteReservation.class,
        FindFlights.class,
        FindOpenSeats.class,
        NewReservation.class,
        UpdateCustomer.class,
        UpdateReservation.class,
    };
    
    // Transaction Frequencies
    {
        addTransactionFrequency(DeleteReservation.class, AirlineConstants.FREQUENCY_DELETE_RESERVATION);
        addTransactionFrequency(FindFlights.class, AirlineConstants.FREQUENCY_FIND_FLIGHTS);
        addTransactionFrequency(FindOpenSeats.class, AirlineConstants.FREQUENCY_FIND_OPEN_SEATS);
        addTransactionFrequency(NewReservation.class, AirlineConstants.FREQUENCY_NEW_RESERVATION);
        addTransactionFrequency(UpdateCustomer.class, AirlineConstants.FREQUENCY_UPDATE_CUSTOMER);
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
