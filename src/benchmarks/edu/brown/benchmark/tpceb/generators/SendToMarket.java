package edu.brown.benchmark.tpceb.generators;

import edu.brown.benchmark.tpceb.TPCEConstants.eMEETradeRequestAction;

public class SendToMarket extends TxnHarnessSendtoMarketInterface{

    private MEE marketExchangeEmulator;
    
    public SendToMarket(MEE marketExchangeEmulator){
        this.marketExchangeEmulator = marketExchangeEmulator;
    }
    public MEE getMEE(){
        return this.marketExchangeEmulator;
    }
    
    public boolean SendToMarketFromFrame(TTradeRequest trade_mes)
    {
        trade_mes.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        return SendToMarket(trade_mes);
    }

    public boolean SendToMarketFromHarness(TTradeRequest trade_mes)
    {
        return SendToMarket(trade_mes);
    }
    
    protected boolean SendToMarket(TTradeRequest trade_mes) {
        marketExchangeEmulator.submitTradeRequest(trade_mes);
        return true;
    }

}
