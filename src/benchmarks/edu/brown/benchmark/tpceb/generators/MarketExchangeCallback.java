package edu.brown.benchmark.tpceb.generators;

import java.util.LinkedList;

public class MarketExchangeCallback extends MEESUTInterface{
    //public LinkedList<TTradeResultTxnInput> inputs = new LinkedList<TTradeResultTxnInput>();
    public LinkedList inputs = new LinkedList();
    public MarketExchangeCallback(TTradeResultTxnInput trTxnInput, TMarketFeedTxnInput mfTxnInput){
        m_TradeResultTxnInput = trTxnInput;
        m_MarketFeedTxnInput = mfTxnInput;
    } 
    
    public boolean TradeResult( TTradeResultTxnInput pTxnInput ) {
        System.out.println("STATUS:" + pTxnInput.st_completed_id);
        System.out.println("IN MARKET EXCHANGE CALLBACK" + pTxnInput.trade_id);
        if(inputs.contains(pTxnInput.trade_id)){
            System.out.println("AddingTID" + pTxnInput.trade_id);
            m_TradeResultTxnInput.trade_id = pTxnInput.trade_id;
            m_TradeResultTxnInput.trade_price = pTxnInput.trade_price;
            m_TradeResultTxnInput.st_completed_id = pTxnInput.st_completed_id;
            inputs.add(pTxnInput.trade_id);
        }
        return (true);
    }
        
    public boolean MarketFeed( TMarketFeedTxnInput pTxnInput ) {
        System.out.println("populating entries");
        for (int i = 0; i < TxnHarnessStructs.max_feed_len; i++) {
            m_MarketFeedTxnInput.Entries[i] = pTxnInput.Entries[i];
        }
        m_MarketFeedTxnInput.StatusAndTradeType = pTxnInput.StatusAndTradeType;
        System.out.println("populating successful");
        return (true);
        }
    

    
    private TTradeResultTxnInput    m_TradeResultTxnInput;
    private TMarketFeedTxnInput     m_MarketFeedTxnInput;
}
