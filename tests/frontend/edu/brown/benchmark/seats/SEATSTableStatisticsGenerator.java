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

import org.voltdb.catalog.Database;

import edu.brown.statistics.AbstractTableStatisticsGenerator;
import edu.brown.utils.ProjectType;

/**
 * Airline Initial Table Sizes
 * 
 * @author pavlo
 */
public class SEATSTableStatisticsGenerator extends AbstractTableStatisticsGenerator {
    
    /**
     * @param catalogDb
     * @param projectType
     * @param scaleFactor
     */
    public SEATSTableStatisticsGenerator(Database catalog_db, double scale_factor) {
        super(catalog_db, ProjectType.SEATS, scale_factor);
    }

    @Override
    public void createProfiles() {
        TableProfile p = null;
        
        // COUNTRY
        p = new TableProfile(this.catalog_db, SEATSConstants.TABLENAME_COUNTRY, false, 248); // XXX
        this.addTableProfile(p);
        
        // AIRPORT
        p = new TableProfile(this.catalog_db, SEATSConstants.TABLENAME_AIRPORT, false, 286); // XXX
        this.addTableProfile(p);

        // AIRPORT_DISTANCE
        p = new TableProfile(this.catalog_db, SEATSConstants.TABLENAME_AIRPORT_DISTANCE, false, 40755); // XXX
        this.addTableProfile(p);
        
        // AIRLINE 
        p = new TableProfile(this.catalog_db, SEATSConstants.TABLENAME_AIRLINE, false, 5416); // XXX
        this.addTableProfile(p);
        
        // CUSTOMER
        p = new TableProfile(this.catalog_db, SEATSConstants.TABLENAME_CUSTOMER, false, SEATSConstants.CUSTOMERS_COUNT);
        this.addTableProfile(p);
        
        // FREQUENT_FLYER
        p = new TableProfile(this.catalog_db, SEATSConstants.TABLENAME_FREQUENT_FLYER, false);
        p.addMultiplicativeDependency(this.catalog_db, SEATSConstants.TABLENAME_CUSTOMER, 2.5); // ESTIMATE
        this.addTableProfile(p);

        // FLIGHT
        long num_flights = (int)Math.round(SEATSConstants.FLIGHTS_PER_DAY_MAX);
        num_flights *= SEATSConstants.FLIGHTS_DAYS_FUTURE + SEATSConstants.FLIGHTS_DAYS_PAST + 1; 
        p = new TableProfile(this.catalog_db, SEATSConstants.TABLENAME_FLIGHT, false, num_flights);
        this.addTableProfile(p);
        
        // RESERVATION
        p = new TableProfile(this.catalog_db, SEATSConstants.TABLENAME_RESERVATION, false);
        p.addMultiplicativeDependency(this.catalog_db, SEATSConstants.TABLENAME_FLIGHT, SEATSConstants.FLIGHTS_NUM_SEATS); // ESTIMATE
    }
}