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

import org.apache.log4j.Logger;

public class EGenClientDriver {
    private static final Logger LOG = Logger.getLogger(EGenClientDriver.class.getName());
    
    /**
     * Initialize the native object
     * @param configuredCustomerCount
     * @param totalCustomerCount
     * @param scaleFactor
     * @param initialDays
     */
    public native long initialize(String data_path, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays);
    
    private native Object[] egenBrokerVolume(long driver_ptr);
    private native Object[] egenCustomerPosition(long driver_ptr);
    private native Object[] egenMarketWatch(long driver_ptr);
    private native Object[] egenSecurityDetail(long driver_ptr);
    private native Object[] egenTradeLookup(long driver_ptr);
    private native Object[] egenTradeOrder(long driver_ptr);
    private native Object[] egenTradeStatus(long driver_ptr);
    private native Object[] egenTradeUpdate(long driver_ptr);
    
    private final long driver_ptr;
    
    /**
     * Constructor
     * @param egenloader_path
     * @param totalCustomerCount
     * @param scaleFactor
     * @param initialDays
     */
    public EGenClientDriver(String egenloader_path, int totalCustomerCount, int scaleFactor, int initialDays) {
        assert(egenloader_path != null) : "The EGENLOADER_PATH parameter is null";
        assert(!egenloader_path.isEmpty()) : "The EGENLOADER_PATH parameter is empty";
        
        File library_path = new File(egenloader_path + File.separator + "lib" + File.separator + "libegen.so"); 
        LOG.debug("Loading in " + EGenClientDriver.class.getSimpleName() + " library '" + library_path + "'");
        try {
            System.load(library_path.getAbsolutePath());
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.fatal("Failed to load " + EGenClientDriver.class.getSimpleName() + " library", ex);
            System.exit(1);
        }
        
        String input_path = new File(egenloader_path + File.separator + "flat_in").getAbsolutePath();
        LOG.debug("Invoking initialization method on driver using data path '" + input_path + "'");
        this.driver_ptr = this.initialize(input_path, totalCustomerCount, totalCustomerCount, scaleFactor, initialDays);
    }
    
    public Object[] getBrokerVolumeParams() {
        return (this.egenBrokerVolume(this.driver_ptr));
    }
    public Object[] getCustomerPositionParams() {
        return (this.egenCustomerPosition(this.driver_ptr));
    }
    public Object[] getMarketWatchParams() {
        return (this.egenMarketWatch(this.driver_ptr));
    }
    public Object[] getSecurityDetailParams() {
        return (this.egenSecurityDetail(this.driver_ptr));
    }
    public Object[] getTradeLookupParams() {
        return (this.egenTradeLookup(this.driver_ptr));
    }
    public Object[] getTradeOrderParams() {
        return (this.egenTradeOrder(this.driver_ptr));
    }
    public Object[] getTradeStatusParams() {
        return (this.egenTradeStatus(this.driver_ptr));
    }
    public Object[] getTradeUpdateParams() {
        return (this.egenTradeUpdate(this.driver_ptr));
    }
}
