/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *  http://www.cs.brown.edu/~zhe/                                          *
 *                                                                         *
 *  Modifications by:                                                      *
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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.tpceb.TPCEConstants;
import edu.brown.benchmark.tpceb.TPCEConstants.DriverType;
import edu.brown.benchmark.tpceb.generators.*;
import edu.brown.benchmark.tpceb.util.RandUtil;
import edu.brown.benchmark.tpceb.util.TableStatistics;

/**
 * @author pavlo
 */
public class TPCEClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(TPCEClient.class);

    public static final int STATUS_SUCCESS = 0;
    public int countTotal =0;

    private static Transaction XTRANS[] = Transaction.values();

    // EGen Drivers
    protected final EGenClientDriver egen_clientDriver;

    private class TPCECallback implements ProcedureCallback {
        private final Transaction t;
        public TPCECallback(Transaction t) {
            this.t = t;
            
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, t.ordinal());//t.ordinal());
        }
    }
 
    /**
     * @author pavlo
     */
    private static class StatsCallback implements ProcedureCallback {
        public final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            LOG.info("Processing statistics callback information");
            VoltTable[] results = clientResponse.getResults();
            for (VoltTable vt : results) {
                //
                // [0] SITE_ID
                // [1] STAT_TIME
                // [2] TABLE_NAME
                // [3] TABLE_TYPE
                // [4] TABLE_ACTIVE_TUPLE_COUNT
                // [5] TABLE_ALLOCATED_TUPLE_COUNT
                // [6] TABLE_DELETED_TUPLE_COUNT
                //
                while (vt.advanceRow()) {
                    String table_name = vt.getString(2);
                    long count = vt.getLong(4);
                    TableStatistics.addTupleCount(table_name, count);
                } // WHILE
            } // FOR
            LOG.info("Finished collecting statistic. Number of tables = " + TableStatistics.getTables().size());
            latch.countDown();
        }
    }

    private static enum Transaction {

         TRADE_ORDER("Trade Order", "TradeOrder", TPCEConstants.FREQUENCY_TRADE_ORDER),
        // MARKET_WATCH("Market Watch", "MarketWatch", TPCEConstants.FREQUENCY_MARKET_WATCH),
         TRADE_RESULT("Trade Result", "TradeResult", TPCEConstants.FREQUENCY_TRADE_RESULT),
         MARKET_FEED("Market Feed", "MarketFeed", TPCEConstants.FREQUENCY_MARKET_FEED);
        

        private Transaction(String displayName, String callName, int weight) {
            this.displayName = displayName;
            this.callName = callName;
            this.weight = weight;
        }

        public final String displayName;
        public final String callName;
        public final int weight;
    }

    /**
     * Constructor
     *
     * @param args
     */
    public TPCEClient(String[] args) {

        super(args);
        if (!m_extraParams.containsKey("TPCE_LOADER_FILES")) {
            LOG.error("Unable to start benchmark. Missing '" + "TPCE_LOADER_FILES" + "' parameter");
            System.exit(1);
        }
        
        int total_customers = TPCEConstants.DEFAULT_NUM_CUSTOMERS;
        if (m_extraParams.containsKey("TPCE_TOTAL_CUSTOMERS")) {
            total_customers = Integer.valueOf(m_extraParams.get("TPCE_TOTAL_CUSTOMERS"));
        }
        
        int scale_factor = TPCEConstants.DEFAULT_SCALE_FACTOR;
        if (m_extraParams.containsKey("TPCE_SCALE_FACTOR")) {
            scale_factor = Integer.valueOf(m_extraParams.get("TPCE_SCALE_FACTOR"));
        }
        
        int initial_days = TPCEConstants.DEFAULT_INITIAL_DAYS;
        if (m_extraParams.containsKey("TPCE_INITIAL_DAYS")) {
            initial_days = Integer.valueOf(m_extraParams.get("TPCE_INITIAL_DAYS"));
        }

        this.egen_clientDriver = new EGenClientDriver(m_extraParams.get("TPCE_LOADER_FILES"), total_customers, scale_factor, initial_days);
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[XTRANS.length];
        int i = 0;
        for (Transaction transaction : XTRANS) {
            names[i++] = transaction.displayName;
        }
        return names;
    }
private int num = 1;
    protected Transaction selectTransaction() {
        //getNumThreads * 20
        if(countTotal <= 200){ //probably 200
            num = 1;
            System.out.println("Trade Order");
        int iTxnType = egen_clientDriver.driver_ptr.getCE().getCETxnMixGenerator().generateNextTxnType( );
        egen_clientDriver.driver_ptr.getCE().zeroInputBuffer(iTxnType);
       // egen_clientDriver.driver_ptr.getMEE();
        //egen_clientDriver.driver_ptr.getMEE();
        //      return Transaction.TRADE_UPDATE;
        //System.out.println(iTxnType);
        countTotal++;
        
        return XTRANS[iTxnType];
        }
        else{
            num = 2;
            System.out.println("Market Feed");
      //      if(countTotal <= 50){
      //          countTotal++;
      //          egen_clientDriver.driver_ptr.getMEE();
      //         // return null;
                return XTRANS[2];
     //           
            }
     //       else{
     //           countTotal++;
     //           egen_clientDriver.driver_ptr.getMEE();
     //           return XTRANS[1];
                
     //       }
            
     //   }
      /* int iTxnType = egen_clientDriver.driver_ptr.getCE().getCETxnMixGenerator().generateNextTxnType( );
        egen_clientDriver.driver_ptr.getCE().zeroInputBuffer(iTxnType);
        return XTRANS[0];*/
    }
    
  /*  protected Transaction selectTransactionME() {
        int iTxnType = egen_clientDriver.driver_ptr.getMEE().generateTradeResult();
        //egen_clientDriver.driver_ptr.getMEE();
        //      return Transaction.TRADE_UPDATE;
        System.out.println(iTxnType);
        return XTRANS[iTxnType];
      /* int iTxnType = egen_clientDriver.driver_ptr.getCE().getCETxnMixGenerator().generateNextTxnType( );
        egen_clientDriver.driver_ptr.getCE().zeroInputBuffer(iTxnType);
        return XTRANS[0];*/
  ///  }

    /**
     * Main control loop
     */
    @Override
    public void runLoop() {
        int no_connection = 10000;
        try {
            final Transaction target = selectTransaction();

            LOG.debug("Executing txn " + target);
            //TPCECallback temp = new TPCECallback(target);
        
           while (!this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(target))) {
                this.getClientHandle().backpressureBarrier();
            }
        } catch (NoConnectionsException e) {
            if (no_connection % 1000 == 0)
                System.err.println("No connections...");
            if (no_connection-- <= 0) {
                System.err.println("Screw this! I'm leaving! Thanks for nothing!");
                return;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
int countRow =0;
 //   @Override
    protected boolean runOnce() throws IOException {
        boolean ret = false;
      //  boolean retME = false;
        if(num ==1){
            System.out.println("num was 1");
        try {
            final Transaction target = selectTransaction();
           // final Transaction targetME = selectTransactionME();
            tradeRequest = new TTradeRequest();
            LOG.debug("Executing txn " + target);
           // ret = this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(target));
            tradeOrderResult = this.getClientHandle().callProcedure(target.callName, this.generateClientArgs(target)).getResults();
            
           // System.out.println("countRow:"+ countRow + "  " +tradeOrderResult[countRow]);
           // System.out.println(tradeOrderResult[0]);

           //System.out.println(tradeOrderResult.length);
            if(tradeOrderResult.length == 0){
                ret = false;
            }
            else if(tradeOrderResult[0] == null){
                System.out.println("was null");
                   ret = false;
            }
            else if(tradeOrderResult[0].fetchRow(0) == null){
                System.out.println("second option was null");
            }
            else{
              //  if(countRow != 0){
                   //tradeOrderResult[0].advanceRow();
                  // System.out.println("Active index"+ tradeOrderResult[0].getActiveRowIndex());
               //    tradeOrderResult[0].advanceRow();
                  // System.out.println("after advance"+ tradeOrderResult[0].getActiveRowIndex());
                  //  tradeOrderResult[0].resetRowPosition();
                  // System.out.println("after reset"+ tradeOrderResult[0].getActiveRowIndex());
                  // tradeOrderResult[0].advanceRow();
                  // System.out.println("after next advance"+ tradeOrderResult[0].getActiveRowIndex());
                  // System.out.println(tradeOrderResult[0].advanceToRow(0));
             //   }
              // System.out.println(tradeOrderResult[0]);
               // tradeOrderResult[0].advanceToRow(0);
               // System.out.println("not null");
              //  System.out.println("val" + tradeOrderResult[0].fetchRow(0));
                tradeRequest.price_quote = tradeOrderResult[0].fetchRow(0).getDouble("price_quote");
               // System.out.println("price_quote " + tradeOrderResult[0].fetchRow(0).getDouble("price_quote"));
                
                tradeRequest.trade_qty = (int) tradeOrderResult[0].fetchRow(0).getDouble("trade_qty");
               // System.out.println("trade_qty " + tradeOrderResult[0].fetchRow(0).getDouble("trade_qty"));
                
                tradeRequest.eActionTemp = (int) tradeOrderResult[0].fetchRow(0).getDouble("eAction");
                //System.out.println("eActionTemp " + tradeOrderResult[0].fetchRow(0).getDouble("eAction"));
                
                tradeRequest.symbol = tradeOrderResult[0].fetchRow(0).getString("symbol");
                //System.out.println("symbol " + tradeOrderResult[0].fetchRow(0).getString("symbol"));
                
                tradeRequest.trade_id = tradeOrderResult[0].fetchRow(0).getLong("trade_id");
                //System.out.println("trade_id " + tradeOrderResult[0].fetchRow(0).getLong("trade_id"));
                
                tradeRequest.trade_type_id = tradeOrderResult[0].fetchRow(0).getString("trade_type_id");
                //System.out.println("trade_type_id " + tradeOrderResult[0].fetchRow(0).getString("trade_type_id"));
                tradeRequest.eAction = null;
                ret = true;
                egen_clientDriver.driver_ptr.getMEE().submitTradeRequest(tradeRequest);
                countRow++;
               // System.out.println("true");
            }
          
            // clientResponse.getResults();
            //  tradeOrderResult = this.getClientHandle().getResults();// retME = this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(targetME));
        } 
        catch (ProcCallException ex) {
            ex.printStackTrace();
        }
        catch (Exception ex) {
        
            ex.printStackTrace();
            System.exit(1);
        }
        }
        else{
            
            try {
                final Transaction target = selectTransaction();
               // final Transaction targetME = selectTransactionME();
                
                LOG.debug("Executing txn " + target);
               ret = this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(target));
               // tradeOrderResult = this.getClientHandle().callProcedure(target.callName, this.generateClientArgs(target)).getResults();
              
                
              
                // clientResponse.getResults();
                //  tradeOrderResult = this.getClientHandle().getResults();// retME = this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(targetME));
            } 
            catch (Exception ex) {
            
                ex.printStackTrace();
                System.exit(1);
            }  
        }

        return ret;
    }


    /**
     * For a given transaction type, use the EGenClientDriver to generate input
     * parameters for execution
     *
     * @param xact
     * @return
     */
    private Object[] generateClientArgs(Transaction xact) {
        switch (xact) {
       /* case MARKET_WATCH:
            return (this.egen_clientDriver.getMarketWatchParams());*/
        case TRADE_ORDER:
            return (this.egen_clientDriver.getTradeOrderParams());
        case MARKET_FEED:
            return (this.egen_clientDriver.getMarketFeedParams());
        case TRADE_RESULT:
            return (this.egen_clientDriver.getTradeResultParams());
     
        default:
            LOG.error("Unsupported client transaction: " + xact);
        }
        return (null);
          //  return (this.egen_clientDriver.getTradeOrderParams());
       // return (this.egen_clientDriver.getMarketWatchParams());
      /*  case MARKET_WATCH:
            return (this.egen_clientDriver.getMarketWatchParams());*/
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        BenchmarkComponent.main(TPCEClient.class, args, false);
    }
    private TTradeRequest tradeRequest;
    private VoltTable[] tradeOrderResult;
}


/*package edu.brown.benchmark.tpceb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.tpceb.TPCEConstants.DriverType;
import edu.brown.benchmark.tpceb.generators.*;
import edu.brown.benchmark.tpceb.util.RandUtil;
import edu.brown.benchmark.tpceb.util.TableStatistics;

/**
 * @author pavlo
 */
/*public class TPCEClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(TPCEClient.class);

    public static final int STATUS_SUCCESS = 0;

    private static Transaction XTRANS[] = Transaction.values();

    // EGen Drivers
    protected final EGenClientDriver egen_clientDriver;

    private class TPCECallback implements ProcedureCallback {
        private final Transaction t;
        public TPCECallback(Transaction t) {
            this.t = t;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 0);//t.ordinal());
        }
    }
 
    /**
     * @author pavlo
     */
 /*   private static class StatsCallback implements ProcedureCallback {
        public final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            LOG.info("Processing statistics callback information");
            VoltTable[] results = clientResponse.getResults();
            for (VoltTable vt : results) {
                //
                // [0] SITE_ID
                // [1] STAT_TIME
                // [2] TABLE_NAME
                // [3] TABLE_TYPE
                // [4] TABLE_ACTIVE_TUPLE_COUNT
                // [5] TABLE_ALLOCATED_TUPLE_COUNT
                // [6] TABLE_DELETED_TUPLE_COUNT
                //
                while (vt.advanceRow()) {
                    String table_name = vt.getString(2);
                    long count = vt.getLong(4);
                    TableStatistics.addTupleCount(table_name, count);
                } // WHILE
            } // FOR
            LOG.info("Finished collecting statistic. Number of tables = " + TableStatistics.getTables().size());
            latch.countDown();
        }
    }

    private static enum Transaction {

        TRADE_ORDER("Trade Order", "TradeOrder", TPCEConstants.FREQUENCY_TRADE_ORDER);


        private Transaction(String displayName, String callName, int weight) {
            this.displayName = displayName;
            this.callName = callName;
            this.weight = weight;
        }

        public final String displayName;
        public final String callName;
        public final int weight;
    }

    /**
     * Constructor
     *
     * @param args
     */
   /* public TPCEClient(String[] args) {

        super(args);
        if (!m_extraParams.containsKey("TPCE_LOADER_FILES")) {
            LOG.error("Unable to start benchmark. Missing '" + "TPCE_LOADER_FILES" + "' parameter");
            System.exit(1);
        }
        
        int total_customers = TPCEConstants.DEFAULT_NUM_CUSTOMERS;
        if (m_extraParams.containsKey("TPCE_TOTAL_CUSTOMERS")) {
            total_customers = Integer.valueOf(m_extraParams.get("TPCE_TOTAL_CUSTOMERS"));
        }
        
        int scale_factor = TPCEConstants.DEFAULT_SCALE_FACTOR;
        if (m_extraParams.containsKey("TPCE_SCALE_FACTOR")) {
            scale_factor = Integer.valueOf(m_extraParams.get("TPCE_SCALE_FACTOR"));
        }
        
        int initial_days = TPCEConstants.DEFAULT_INITIAL_DAYS;
        if (m_extraParams.containsKey("TPCE_INITIAL_DAYS")) {
            initial_days = Integer.valueOf(m_extraParams.get("TPCE_INITIAL_DAYS"));
        }

        this.egen_clientDriver = new EGenClientDriver(m_extraParams.get("TPCE_LOADER_FILES"), total_customers, scale_factor, initial_days);
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[XTRANS.length];
        int i = 0;
        for (Transaction transaction : XTRANS) {
            names[i++] = transaction.displayName;
        }
        return names;
    }

    protected Transaction selectTransaction() {
        
       int iTxnType = egen_clientDriver.driver_ptr.getCE().getCETxnMixGenerator().generateNextTxnType( );
        egen_clientDriver.driver_ptr.getCE().zeroInputBuffer(iTxnType);
        return XTRANS[0];
    }

    /**
     * Main control loop
     */
  //  @Override
  /*  public void runLoop() {
        int no_connection = 10000;
        try {
            final Transaction target = selectTransaction();

            LOG.debug("Executing txn " + target);

           while (!this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(target))) {
                this.getClientHandle().backpressureBarrier();
            }
        } catch (NoConnectionsException e) {
            if (no_connection % 1000 == 0)
                System.err.println("No connections...");
            if (no_connection-- <= 0) {
                System.err.println("Screw this! I'm leaving! Thanks for nothing!");
                return;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
//int countNumRuns =0;
 //   @Override
    protected boolean runOnce() throws IOException {
        boolean ret = false;
        try {
            final Transaction target = selectTransaction();

            LOG.debug("Executing txn " + target);
            ret = this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(target));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        return ret;
    }


    /**
     * For a given transaction type, use the EGenClientDriver to generate input
     * parameters for execution
     *
     * @param xact
     * @return
     */
  /*  private Object[] generateClientArgs(Transaction xact) {
            return (this.egen_clientDriver.getTradeOrderParams());
    }

    /**
     * @param args
     */
  /*  public static void main(String[] args) {
        BenchmarkComponent.main(TPCEClient.class, args, false);
    }
}*/
