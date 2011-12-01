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
    
    // Config Management
    {
        this.addStmtProcedure("LoadConfigProfile",    "SELECT * FROM " + AirlineConstants.TABLENAME_CONFIG_PROFILE);
        this.addStmtProcedure("LoadConfigHistograms", "SELECT * FROM " + AirlineConstants.TABLENAME_CONFIG_HISTOGRAMS);
    }
    
    // Transaction Frequencies
    {
        addTransactionFrequency(DeleteReservation.class, AirlineConstants.FREQUENCY_DELETE_RESERVATION);
        addTransactionFrequency(FindFlights.class, AirlineConstants.FREQUENCY_FIND_FLIGHTS);
        addTransactionFrequency(FindOpenSeats.class, AirlineConstants.FREQUENCY_FIND_OPEN_SEATS);
        addTransactionFrequency(NewReservation.class, AirlineConstants.FREQUENCY_NEW_RESERVATION);
        addTransactionFrequency(UpdateCustomer.class, AirlineConstants.FREQUENCY_UPDATE_CUSTOMER);
        addTransactionFrequency(UpdateReservation.class, AirlineConstants.FREQUENCY_UPDATE_RESERVATION);
    }
    
    // Vertical Partitions
    {
//        addVerticalPartitionInfo(AirlineConstants.TABLENAME_CUSTOMER, "C_ID", "C_ID_STR");
    }
    
    public static final String PARTITIONING[][] = 
        new String[][] {
            {AirlineConstants.TABLENAME_CUSTOMER,      "C_ID"},
            {AirlineConstants.TABLENAME_FLIGHT,        "F_ID"},
            {AirlineConstants.TABLENAME_RESERVATION,   "R_F_ID"},
            
            // Config Tables
            {AirlineConstants.TABLENAME_CONFIG_PROFILE ,    "CFP_SCALE_FACTOR"},
            {AirlineConstants.TABLENAME_CONFIG_HISTOGRAMS , "CFH_NAME"},
            
        };

    public AirlineProjectBuilder() {
        super("airline", AirlineProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}
