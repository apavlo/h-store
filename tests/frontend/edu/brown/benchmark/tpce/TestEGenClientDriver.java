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

import org.voltdb.VoltType;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

public class TestEGenClientDriver extends BaseTestCase {

    // HACK
    private static final String EGENLOADER_HOME = System.getenv("TPCE_LOADER_FILES");
    protected static EGenClientDriver driver;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCE, true, true);
        if (driver == null) {
            driver = new EGenClientDriver(EGENLOADER_HOME, TPCEConstants.DEFAULT_NUM_CUSTOMERS, TPCEConstants.DEFAULT_SCALE_FACTOR, TPCEConstants.DEFAULT_INITIAL_DAYS);
        }
    }

    private void checkParamTypes(Procedure catalog_proc, Object params[]) throws Exception {
        System.err.println(StringUtil.DOUBLE_LINE);
        System.err.println(catalog_proc);

        assertEquals(catalog_proc.getParameters().size(), params.length);
        for (int i = 0, cnt = params.length; i < cnt; i++) {
            ProcParameter catalog_param = catalog_proc.getParameters().get(i);
            assertNotNull(catalog_param);

            try {
                VoltType expected_type = VoltType.get((byte) catalog_param.getType());
                if (catalog_param.getIsarray()) {
                    int j = 0;
                    for (Object inner : (Object[]) params[i]) {
                        System.err.print("[" + i + "][" + j++ + "]: " + expected_type + " --> " + inner + " [class=" + inner.getClass() + ",");
                        VoltType param_type = VoltType.typeFromClass(inner.getClass());
                        System.err.println("type=" + param_type + "]");
                        assertEquals(expected_type, param_type);
                    } // FOR
                } else {
                    System.err.print("[" + i + "]: " + expected_type + " --> " + params[i] + " [class=" + params[i].getClass() + ",");
                    VoltType param_type = VoltType.typeFromClass(params[i].getClass());
                    System.err.println("type=" + param_type + "]");
                    assertEquals(expected_type, param_type);
                }
            } catch (NullPointerException ex) {
                System.err.println("Null parameter at index " + i + " for " + catalog_proc);
                throw ex;
            }
        } // FOR
    }

    /**
     * testGetBrokerVolumeParams
     */
    public void testGetBrokerVolumeParams() throws Exception {
        Object params[] = driver.getBrokerVolumeParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("BrokerVolume");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    /**
     * testGetCustomerPositionParams
     */
    public void testGetCustomerPositionParams() throws Exception {
        Object params[] = driver.getCustomerPositionParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("CustomerPosition");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    /**
     * testGetDataMaintenanceParams
     */
    public void testGetDataMaintenanceParams() throws Exception {
        Object params[] = driver.getDataMaintenanceParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("DataMaintenance");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    // /**
    // * testGetMarketFeedParams
    // */
    // public void testGetMarketFeedParams() throws Exception {
    // Object params[] = driver.getMarketFeedParams();
    // assertNotNull(params);
    //
    // Procedure catalog_proc = this.getProcedure("MarketFeed");
    // assertNotNull(catalog_proc);
    // this.checkParamTypes(catalog_proc, params);
    // }

    /**
     * testGetMarketWatchParams
     */
    public void testGetMarketWatchParams() throws Exception {
        Object params[] = driver.getMarketWatchParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("MarketWatch");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    /**
     * testGetSecurityDetailParams
     */
    public void testGetSecurityDetailParams() throws Exception {
        Object params[] = driver.getSecurityDetailParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("SecurityDetail");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    /**
     * testGetTradeCleanupParams
     */
    public void testGetTradeCleanupParams() throws Exception {
        Object params[] = driver.getTradeCleanupParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("TradeCleanup");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    /**
     * testGetTradeLookupParams
     */
    public void testGetTradeLookupParams() throws Exception {
        Object params[] = driver.getTradeLookupParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("TradeLookup");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    /**
     * testGetTradeOrderParams
     */
    public void testGetTradeOrderParams() throws Exception {
        Object params[] = driver.getTradeOrderParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("TradeOrder");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    // /**
    // * testGetTradeResultParams
    // */
    // public void testGetTradeResultParams() throws Exception {
    // Object params[] = driver.getTradeResultParams();
    // assertNotNull(params);
    //
    // Procedure catalog_proc = this.getProcedure("TradeResult");
    // assertNotNull(catalog_proc);
    // this.checkParamTypes(catalog_proc, params);
    // }

    /**
     * testGetTradeStatusParams
     */
    public void testGetTradeStatusParams() throws Exception {
        Object params[] = driver.getTradeStatusParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("TradeStatus");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }

    /**
     * testGetTradeUpdateParams
     */
    public void testGetTradeUpdateParams() throws Exception {
        Object params[] = driver.getTradeUpdateParams();
        assertNotNull(params);

        Procedure catalog_proc = this.getProcedure("TradeUpdate");
        assertNotNull(catalog_proc);
        this.checkParamTypes(catalog_proc, params);
    }
}