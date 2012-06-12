package edu.brown.benchmark.tpce.generators;

public class TMarketFeedTxnInput {
    public TStatusAndTradeType   StatusAndTradeType;
    public char[]                zz_padding;
    public TTickerEntry[]        Entries;
    
    public TMarketFeedTxnInput(){
        StatusAndTradeType = new TStatusAndTradeType();
        zz_padding = new char[4];
        Entries = new TTickerEntry[TxnHarnessStructs.max_feed_len];
    }
}
