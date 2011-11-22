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

import org.voltdb.catalog.Database;

import edu.brown.statistics.AbstractTableStatisticsGenerator;
import edu.brown.utils.ProjectType;

/**
 * Airline Initial Table Sizes
 * 
 * @author pavlo
 */
public class AirlineTableStatisticsGenerator extends AbstractTableStatisticsGenerator {
    
    /**
     * @param catalogDb
     * @param projectType
     * @param scaleFactor
     */
    public AirlineTableStatisticsGenerator(Database catalog_db, double scale_factor) {
        super(catalog_db, ProjectType.AIRLINE, scale_factor);
    }

    @Override
    public void createProfiles() {
        TableProfile p = null;
        
        // COUNTRY
        p = new TableProfile(this.catalog_db, AirlineConstants.TABLENAME_COUNTRY, false, 248); // XXX
        this.addTableProfile(p);
        
        // AIRPORT
        p = new TableProfile(this.catalog_db, AirlineConstants.TABLENAME_AIRPORT, false, 286); // XXX
        this.addTableProfile(p);

        // AIRPORT_DISTANCE
        p = new TableProfile(this.catalog_db, AirlineConstants.TABLENAME_AIRPORT_DISTANCE, false, 40755); // XXX
        this.addTableProfile(p);
        
        // AIRLINE 
        p = new TableProfile(this.catalog_db, AirlineConstants.TABLENAME_AIRLINE, false, 5416); // XXX
        this.addTableProfile(p);
        
        // CUSTOMER
        p = new TableProfile(this.catalog_db, AirlineConstants.TABLENAME_CUSTOMER, false, AirlineConstants.NUM_CUSTOMERS);
        this.addTableProfile(p);
        
        // FREQUENT_FLYER
        p = new TableProfile(this.catalog_db, AirlineConstants.TABLENAME_FREQUENT_FLYER, false);
        p.addMultiplicativeDependency(this.catalog_db, AirlineConstants.TABLENAME_CUSTOMER, 2.5); // ESTIMATE
        this.addTableProfile(p);

        // FLIGHT
        long num_flights = (int)Math.round(AirlineConstants.NUM_FLIGHTS_PER_DAY * 1.25);
        num_flights *= AirlineConstants.DAYS_FUTURE + AirlineConstants.DAYS_PAST + 1; 
        p = new TableProfile(this.catalog_db, AirlineConstants.TABLENAME_FLIGHT, false, num_flights);
        this.addTableProfile(p);
        
        // RESERVATION
        p = new TableProfile(this.catalog_db, AirlineConstants.TABLENAME_RESERVATION, false);
        p.addMultiplicativeDependency(this.catalog_db, AirlineConstants.TABLENAME_FLIGHT, AirlineConstants.NUM_SEATS_PER_FLIGHT); // ESTIMATE
    }
}