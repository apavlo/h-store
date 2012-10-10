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
package edu.brown.benchmark.seats;

import java.io.File;
import java.net.URL;

import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.seats.procedures.*;

public class SEATSProjectBuilder extends AbstractProjectBuilder {
    
    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends BenchmarkComponent> m_clientClass = SEATSClient.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SEATSLoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        DeleteReservation.class,
        FindFlights.class,
        FindOpenSeats.class,
        NewReservation.class,
        UpdateCustomer.class,
        UpdateReservation.class,
        
        // Non-Workload Procedure
        LoadConfig.class,
        GetTableCounts.class
    };
    
    {
        // Transaction Frequencies
        addTransactionFrequency(DeleteReservation.class, SEATSConstants.FREQUENCY_DELETE_RESERVATION);
        addTransactionFrequency(FindFlights.class, SEATSConstants.FREQUENCY_FIND_FLIGHTS);
        addTransactionFrequency(FindOpenSeats.class, SEATSConstants.FREQUENCY_FIND_OPEN_SEATS);
        addTransactionFrequency(NewReservation.class, SEATSConstants.FREQUENCY_NEW_RESERVATION);
        addTransactionFrequency(UpdateCustomer.class, SEATSConstants.FREQUENCY_UPDATE_CUSTOMER);
        addTransactionFrequency(UpdateReservation.class, SEATSConstants.FREQUENCY_UPDATE_RESERVATION);
    
        // Replicated Secondary Index
        addReplicatedSecondaryIndex(SEATSConstants.TABLENAME_CUSTOMER, "C_ID", "C_ID_STR");
    }
    
    public static final String PARTITIONING[][] = 
        new String[][] {
            {SEATSConstants.TABLENAME_CUSTOMER,       "C_ID"},
            {SEATSConstants.TABLENAME_FLIGHT,         "F_ID"},
            {SEATSConstants.TABLENAME_FREQUENT_FLYER, "FF_C_ID"},
            {SEATSConstants.TABLENAME_RESERVATION,    "R_F_ID"},
            
            // Config Tables
            {SEATSConstants.TABLENAME_CONFIG_PROFILE ,    "CFP_SCALE_FACTOR"},
            {SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS , "CFH_NAME"},
            
        };

    public SEATSProjectBuilder() {
        super("seats", SEATSProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
    
    public static File getDataDir() {
        URL url = SEATSProjectBuilder.class.getResource("data");
        if (url != null) {
            return new File(url.getPath());
        }
        return (null);
    }
    
    /**
     * Return the path of the CSV file that has data for the given Table catalog handle
     * @param data_dir
     * @param catalog_tbl
     * @return
     */
    public static final File getTableDataFile(File data_dir, Table catalog_tbl) {
        File f = new File(String.format("%s%stable.%s.csv", data_dir.getAbsolutePath(),
                                                            File.separator,
                                                            catalog_tbl.getName().toLowerCase()));
        if (f.exists() == false) f = new File(f.getAbsolutePath() + ".gz");
        return (f);
    }
}
