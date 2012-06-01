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
    	
	System.out.println("EGenClientDriver: line: 128: acct_id_idx: " + obj[0].toString());
	System.out.println("EGenClientDriver: line: 129: cust_id: " + obj[1].toString());
	System.out.println("EGenClientDriver: line: 130: get_history: " + obj[2].toString());
	System.out.println("EGenClientDriver: line: 131: tax_id: " + obj[3]);
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
	System.out.println("EGenClientDriver: line: 146: acct_id: " + obj[0].toString());
	System.out.println("EGenClientDriver: line: 147: c_id: " + obj[1].toString());
	System.out.println("EGenClientDriver: line: 148: ending_co_id: " + obj[2].toString());
	System.out.println("EGenClientDriver: line: 149: starting_co_id: " + obj[3].toString());
	System.out.println("EGenClientDriver: line: 150: industry_name: " + obj[4].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getSecurityDetailParams() {
    	Object[] obj = driver_ptr.generateSecurityDetailInput().InputParameters().toArray();
	System.out.println("EGenClientDriver: line: 156: max_rows_to_return: " + obj[0].toString());
	System.out.println("EGenClientDriver: line: 157: access_lob_flag: " + obj[1].toString());
	System.out.println("EGenClientDriver: line: 158: start_day: " + obj[2].toString());
	System.out.println("EGenClientDriver: line: 159: symbol: " + obj[3].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getTradeCleanupParams() {
        return (this.cleanParams(driver_ptr.generateTradeCleanupInput().InputParameters().toArray()));
    }

    public Object[] getTradeLookupParams() {
    	Object[] obj = driver_ptr.generateTradeLookupInput().InputParameters().toArray();
	System.out.println("EGenClientDriver: line: 169: trade_id: " + obj[0]);
	System.out.println("EGenClientDriver: line: 170: acct_id: " + obj[1]);
	System.out.println("EGenClientDriver: line: 171: max_acct_id: " + obj[2]);
	System.out.println("EGenClientDriver: line: 172: frame_to_execute: " + obj[3]);
	System.out.println("EGenClientDriver: line: 173: max_trades: " + obj[4]);
	System.out.println("EGenClientDriver: line: 174: end_trade_dts: " + obj[5].toString());
	System.out.println("EGenClientDriver: line: 175: start_trade_dts: " + obj[6].toString());
	System.out.println("EGenClientDriver: line: 176: symbol: " + obj[7].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getTradeOrderParams() {
    	int   iTradeType = 0;
        boolean    bExecutorIsAccountOwner = true;
        Object[] obj = driver_ptr.generateTradeOrderInput(iTradeType, bExecutorIsAccountOwner).InputParameters().toArray();
        System.out.println("EGenClientDriver: line: 184: requested_price: " + obj[0]);
        System.out.println("EGenClientDriver: line: 185: acct_id: " + obj[1]);
        System.out.println("EGenClientDriver: line: 186: is_lifo: " + obj[2]);
        System.out.println("EGenClientDriver: line: 187: roll_it_back: " + obj[3]);
        System.out.println("EGenClientDriver: line: 188: trade_qty: " + obj[4]);
        System.out.println("EGenClientDriver: line: 189: type_is_margin: " + obj[5]);
        System.out.println("EGenClientDriver: line: 190: co_name: " + obj[6]);
        System.out.println("EGenClientDriver: line: 191: exec_f_name: " + obj[7]);
        System.out.println("EGenClientDriver: line: 192: exec_l_name: " + obj[8]);
        System.out.println("EGenClientDriver: line: 193: exec_tax_id: " + obj[9]);
        System.out.println("EGenClientDriver: line: 194: issue: " + obj[10]);
        System.out.println("EGenClientDriver: line: 195: st_pending_id: " + obj[11]);
        System.out.println("EGenClientDriver: line: 196: st_submitted_id: " + obj[12]);
        System.out.println("EGenClientDriver: line: 197: symbol: " + obj[13]);
        System.out.println("EGenClientDriver: line: 198: trade_type_id: " + obj[14]);
        return (this.cleanParams(obj));
    }

   public Object[] getTradeResultParams() {
        return (this.cleanParams(driver_ptr.generateTradeResultInput().InputParameters().toArray()));
    }

    public Object[] getTradeStatusParams() {
    	Object[] obj = driver_ptr.generateTradeStatusInput().InputParameters().toArray();
    	System.out.println("EGenClientDriver: line: 206: acct_id: " + obj[0]);
        return (this.cleanParams(obj));
    }

    public Object[] getTradeUpdateParams() {
    	Object[] obj = driver_ptr.generateTradeUpdateInput().InputParameters().toArray();
    	System.out.println("EGenClientDriver: line: 214: trade_id: " + obj[0]);
        System.out.println("EGenClientDriver: line: 215: acct_id: " + obj[1]);
        System.out.println("EGenClientDriver: line: 216: max_acct_id: " + obj[2]);
        System.out.println("EGenClientDriver: line: 217: frame_to_execute: " + obj[3]);
        System.out.println("EGenClientDriver: line: 218: max_trades: " + obj[4]);
        System.out.println("EGenClientDriver: line: 219: max_updates: " + obj[5]);
        System.out.println("EGenClientDriver: line: 220: end_trade_dts: " + obj[6]);
        System.out.println("EGenClientDriver: line: 221: start_trade_dts: " + obj[7]);
        System.out.println("EGenClientDriver: line: 222: symbol: " + obj[8]);
        return (this.cleanParams(obj));
    }
}