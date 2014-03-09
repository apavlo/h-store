package edu.brown.benchmark.tpceb.generators;

import edu.brown.benchmark.tpceb.TPCEConstants.DriverType;
import edu.brown.benchmark.tpceb.TPCEConstants.eMEETradeRequestAction;

public abstract class TxnHarnessSendtoMarketInterface {
   
    
    protected abstract boolean SendToMarketFromFrame(TTradeRequest trade_mes);

    protected abstract boolean SendToMarketFromHarness(TTradeRequest trade_mes);

    protected abstract boolean SendToMarket(TTradeRequest trade_mes);
}
