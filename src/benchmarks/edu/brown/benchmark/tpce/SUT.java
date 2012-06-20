package edu.brown.benchmark.tpce;

import edu.brown.benchmark.tpce.generators.CESUTInterface;
import edu.brown.benchmark.tpce.generators.TBrokerVolumeTxnInput;
import edu.brown.benchmark.tpce.generators.TCustomerPositionTxnInput;
import edu.brown.benchmark.tpce.generators.TMarketWatchTxnInput;
import edu.brown.benchmark.tpce.generators.TSecurityDetailTxnInput;
import edu.brown.benchmark.tpce.generators.TTradeLookupTxnInput;
import edu.brown.benchmark.tpce.generators.TTradeOrderTxnInput;
import edu.brown.benchmark.tpce.generators.TTradeStatusTxnInput;
import edu.brown.benchmark.tpce.generators.TTradeUpdateTxnInput;

public class SUT extends CESUTInterface{
	public boolean BrokerVolume( TBrokerVolumeTxnInput pTxnInput ){
		return true;
	}
    public  boolean CustomerPosition( TCustomerPositionTxnInput pTxnInput ){
    	return true;
    }
    public  boolean MarketWatch( TMarketWatchTxnInput pTxnInput ){
    	return true;
    }
    public  boolean SecurityDetail( TSecurityDetailTxnInput pTxnInput ){
    	return true;
    }
    public  boolean TradeLookup( TTradeLookupTxnInput pTxnInput ){
    	return true;
    }
    public  boolean TradeOrder( TTradeOrderTxnInput pTxnInput, int iTradeType, boolean bExecutorIsAccountOwner ){
    	return true;
    }
    public  boolean TradeStatus( TTradeStatusTxnInput pTxnInput ){
    	return true;
    }
    public  boolean TradeUpdate( TTradeUpdateTxnInput pTxnInput ){
    	return true;
    }
} 
