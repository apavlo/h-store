
/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
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
package edu.brown.benchmark.tpceb;

import java.io.File;
import java.util.Date;

import org.apache.log4j.Logger;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpceb.generators.BaseLogger;
import edu.brown.benchmark.tpceb.generators.SendToMarket;

public class EGenClientDriver {
    private static final Logger LOG = Logger.getLogger(EGenClientDriver.class.getName());

    /**
     * Initialize the native object
     * 
     * @param configuredCustomerCount
     * @param totalCustomerCount
     * @param scaleFactor
     * @param initialDays
     */


    public ClientDriver driver_ptr;

    /**
     * Constructor
     * 
     * @param egenloader_path
     * @param totalCustomerCount
     * @param scaleFactor
     * @param initialDays
     */
    public EGenClientDriver(String egenloader_path, int totalCustomerCount, int scaleFactor, int initialDays) {
        assert (egenloader_path != null) : "The EGENLOADER_PATH parameter is null";
        assert (!egenloader_path.isEmpty()) : "The EGENLOADER_PATH parameter is empty";

        String input_path = new File(egenloader_path + File.separator).getAbsolutePath();
        LOG.debug("Invoking initialization method on driver using data path '" + input_path + "'");
        driver_ptr = new ClientDriver(input_path, totalCustomerCount, totalCustomerCount, scaleFactor, initialDays);
    }

    private Object[] cleanParams(Object[] orig) {
        // We need to switch java.util.Dates to the stupid volt TimestampType
        for (int i = 0; i < orig.length; i++) {
            if (orig[i] instanceof Date) {
                orig[i] = new TimestampType(((Date) orig[i]).getTime());
            }
        } // FOR
        return (orig);
    }


    public Object[] getTradeOrderParams() {
        int   iTradeType = 0; 
        Object[] obj = driver_ptr.generateTradeOrderInput(iTradeType ).InputParameters().toArray();
      
        return (this.cleanParams(obj));
    }
   
    public Object[] getMarketWatchParams() {
        Object[] obj = driver_ptr.generateMarketWatchInput().InputParameters().toArray();
        return (this.cleanParams(obj));
    }
    
    public Object[] getTradeResultParams() {
        return (this.cleanParams(driver_ptr.generateTradeResultInput().InputParameters().toArray()));
    }
    
    public Object[] getMarketFeedParams() {
        return (this.cleanParams(driver_ptr.generateMarketFeedInput().InputParameters().toArray()));
    }


}

/*OLD CODE FOR TRADE ORDER ONLY - REFERENCE*/
/* package edu.brown.benchmark.tpceb;

import java.io.File;
import java.util.Date;

import org.apache.log4j.Logger;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpceb.generators.BaseLogger;

public class EGenClientDriver {
    private static final Logger LOG = Logger.getLogger(EGenClientDriver.class.getName());

    /**
     * Initialize the native object
     * 
     * @param configuredCustomerCount
     * @param totalCustomerCount
     * @param scaleFactor
     * @param initialDays
     */


 /*   public ClientDriver driver_ptr;

    /**
     * Constructor
     * 
     * @param egenloader_path
     * @param totalCustomerCount
     * @param scaleFactor
     * @param initialDays
     */
  /*  public EGenClientDriver(String egenloader_path, int totalCustomerCount, int scaleFactor, int initialDays) {
        assert (egenloader_path != null) : "The EGENLOADER_PATH parameter is null";
        assert (!egenloader_path.isEmpty()) : "The EGENLOADER_PATH parameter is empty";

        String input_path = new File(egenloader_path + File.separator).getAbsolutePath();
        LOG.debug("Invoking initialization method on driver using data path '" + input_path + "'");
        driver_ptr = new ClientDriver(input_path, totalCustomerCount, totalCustomerCount, scaleFactor, initialDays);
    }

    private Object[] cleanParams(Object[] orig) {
        // We need to switch java.util.Dates to the stupid volt TimestampType
        for (int i = 0; i < orig.length; i++) {
            if (orig[i] instanceof Date) {
                orig[i] = new TimestampType(((Date) orig[i]).getTime());
            }
        } // FOR
        return (orig);
    }


    public Object[] getTradeOrderParams() {
        int   iTradeType = 0; 
        Object[] obj = driver_ptr.generateTradeOrderInput(iTradeType).InputParameters().toArray();
      
        return (this.cleanParams(obj));
    }


}*/