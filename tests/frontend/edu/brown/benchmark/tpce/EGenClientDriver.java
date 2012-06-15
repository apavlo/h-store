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

import edu.brown.benchmark.tpce.generators.BaseLogger;
import edu.brown.benchmark.tpce.generators.TBrokerVolumeTxnInput;

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

    /*
     * for the following System.out.println, they are only for testing whether the parameters are correct or not
     * They can be deleted. And the line number is wrong.
     */
    public Object[] getBrokerVolumeParams() {	
    	Object[] obj = driver_ptr.generateBrokerVolumeInput().InputParameters().toArray();
        return (this.cleanParams(obj));
    }

    public Object[] getCustomerPositionParams() {
    	Object[] obj = driver_ptr.generateCustomerPositionInput().InputParameters().toArray();
    	return (this.cleanParams(obj));
    }

    public Object[] getDataMaintenanceParams() {
        return (this.cleanParams(driver_ptr.generateDataMaintenanceInput().InputParameters().toArray()));
    }

/*    public Object[] getMarketFeedParams() {
        return (this.cleanParams(driver_ptr.generateMarketFeedInput().InputParameters().toArray()));
    }
*/
    public Object[] getMarketWatchParams() {
    	Object[] obj = driver_ptr.generateMarketWatchInput().InputParameters().toArray();
        return (this.cleanParams(obj));
    }

    public Object[] getSecurityDetailParams() {
    	Object[] obj = driver_ptr.generateSecurityDetailInput().InputParameters().toArray();
        return (this.cleanParams(obj));
    }

    public Object[] getTradeCleanupParams() {
        return (this.cleanParams(driver_ptr.generateTradeCleanupInput().InputParameters().toArray()));
    }

    public Object[] getTradeLookupParams() {
    	Object[] obj = driver_ptr.generateTradeLookupInput().InputParameters().toArray();
        return (this.cleanParams(obj));
    }

    public Object[] getTradeOrderParams() {
    	int   iTradeType = 0;
        boolean    bExecutorIsAccountOwner = true;
        Object[] obj = driver_ptr.generateTradeOrderInput(iTradeType, bExecutorIsAccountOwner).InputParameters().toArray();
        return (this.cleanParams(obj));
    }

   public Object[] getTradeResultParams() {
        return (this.cleanParams(driver_ptr.generateTradeResultInput().InputParameters().toArray()));
    }

    public Object[] getTradeStatusParams() {
    	Object[] obj = driver_ptr.generateTradeStatusInput().InputParameters().toArray();
        return (this.cleanParams(obj));
    }

    public Object[] getTradeUpdateParams() {
    	Object[] obj = driver_ptr.generateTradeUpdateInput().InputParameters().toArray();
        return (this.cleanParams(obj));
    }
}