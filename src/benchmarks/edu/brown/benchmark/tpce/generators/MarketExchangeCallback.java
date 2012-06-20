package edu.brown.benchmark.tpce.generators;

public class MarketExchangeCallback extends MEESUTInterface{
    
    public MarketExchangeCallback(TTradeResultTxnInput trTxnInput, TMarketFeedTxnInput mfTxnInput){
        m_TradeResultTxnInput = trTxnInput;
        m_MarketFeedTxnInput = mfTxnInput;
    } 
    
    public boolean TradeResult( TTradeResultTxnInput pTxnInput ) {
        m_TradeResultTxnInput.trade_id = pTxnInput.trade_id;
        m_TradeResultTxnInput.trade_price = pTxnInput.trade_price;
        return (true);
    }
        
    public boolean MarketFeed( TMarketFeedTxnInput pTxnInput ) {
        for (int i = 0; i < TxnHarnessStructs.max_feed_len; i++) {
            m_MarketFeedTxnInput.Entries[i] = pTxnInput.Entries[i];
        }
        m_MarketFeedTxnInput.StatusAndTradeType = pTxnInput.StatusAndTradeType;
        return (true);
        }
    
    private TTradeResultTxnInput    m_TradeResultTxnInput;
    private TMarketFeedTxnInput     m_MarketFeedTxnInput;
}
