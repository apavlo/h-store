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
package edu.brown.benchmark.tpce;

import java.io.File;
import java.util.Date;

import org.apache.log4j.Logger;
import org.voltdb.types.TimestampType;

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
//    public native long initialize(String data_path, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays);

/*    private native Object[] egenBrokerVolume(long driver_ptr);

    private native Object[] egenCustomerPosition(long driver_ptr);

    private native Object[] egenDataMaintenance(long driver_ptr);

    private native Object[] egenMarketFeed(long driver_ptr);

    private native Object[] egenMarketWatch(long driver_ptr);

    private native Object[] egenSecurityDetail(long driver_ptr);

    private native Object[] egenTradeCleanup(long driver_ptr);

    private native Object[] egenTradeLookup(long driver_ptr);

    private native Object[] egenTradeOrder(long driver_ptr);

    private native Object[] egenTradeResult(long driver_ptr);

    private native Object[] egenTradeStatus(long driver_ptr);

    private native Object[] egenTradeUpdate(long driver_ptr);*/

    private ClientDriver driver_ptr;

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

        
	File library_path = new File(egenloader_path + File.separator + "lib" + File.separator + "libegen.so");
        LOG.debug("Loading in " + EGenClientDriver.class.getSimpleName() + " library '" + library_path + "'");
        
	
/*try {
            System.load(library_path.getAbsolutePath());
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.fatal("Failed to load " + EGenClientDriver.class.getSimpleName() + " library", ex);
            System.exit(1);
        }*/

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

    public Object[] getBrokerVolumeParams() {
    	Object[] obj = driver_ptr.generateBrokerVolumeInput().InputParameters().toArray();
    	
    	System.out.println("EGenClientDriver: line: 118: " + obj[0]);
        return (this.cleanParams(obj));
    }

    public Object[] getCustomerPositionParams() {
    	Object[] obj = driver_ptr.generateCustomerPositionInput().InputParameters().toArray();
    	
System.out.println("EGenClientDriver: line: 123: acct_id_idx: " + obj[0].toString());
System.out.println("EGenClientDriver: line: 124: cust_id: " + obj[1].toString());
System.out.println("EGenClientDriver: line: 125: get_history: " + obj[2].toString());
System.out.println("EGenClientDriver: line: 126: tax_id: " + obj[3]);
    	return (this.cleanParams(obj));
//        return (this.cleanParams(driver_ptr.generateCustomerPositionInput().InputParameters().toArray()));
    }

    public Object[] getDataMaintenanceParams() {
        return (this.cleanParams(driver_ptr.generateDataMaintenanceInput().InputParameters().toArray()));
    }

/*    public Object[] getMarketFeedParams() {
        return (this.cleanParams(driver_ptr.generateMarketFeedInput().InputParameters().toArray()));
    }
*/
    public Object[] getMarketWatchParams() {
        return (this.cleanParams(driver_ptr.generateMarketWatchInput().InputParameters().toArray()));
    }

    public Object[] getSecurityDetailParams() {
        return (this.cleanParams(driver_ptr.generateSecurityDetailInput().InputParameters().toArray()));
    }

    public Object[] getTradeCleanupParams() {
        return (this.cleanParams(driver_ptr.generateTradeCleanupInput().InputParameters().toArray()));
    }

    public Object[] getTradeLookupParams() {
        return (this.cleanParams(driver_ptr.generateTradeLookupInput().InputParameters().toArray()));
    }

    public Object[] getTradeOrderParams() {
    	int   iTradeType = 0;
        boolean    bExecutorIsAccountOwner = true;
        return (this.cleanParams(driver_ptr.generateTradeOrderInput(iTradeType, bExecutorIsAccountOwner).InputParameters().toArray()));
    }

/*    public Object[] getTradeResultParams() {
        return (this.cleanParams(driver_ptr.generateTradeResultInput().InputParameters().toArray()));
    }
*/
    public Object[] getTradeStatusParams() {
        return (this.cleanParams(driver_ptr.generateTradeStatusInput().InputParameters().toArray()));
    }

    public Object[] getTradeUpdateParams() {
        return (this.cleanParams(driver_ptr.generateTradeUpdateInput().InputParameters().toArray()));
    }
}