package edu.brown.benchmark.tpce.generators;

import java.util.Date;

import edu.brown.benchmark.tpce.generators.TradeGenerator.TradeType;
import edu.brown.benchmark.tpce.util.EGenDate;
import edu.brown.benchmark.tpce.util.EGenMoney;

public class MEEPriceBoard {
	public MEEPriceBoard( int TradingTimeSoFar, Date pBaseTime, Date pCurrentTime, SecurityHandler pSecurityFile, int configuredCustomerCount){
		m_fMeanInTheMoneySubmissionDelay = 1.0 ;
		m_Security = new MEESecurity();
		m_pSecurityFile = pSecurityFile;
		m_iNumberOfSecurities = 0;
		
		m_iNumberOfSecurities = SecurityHandler.getSecurityNum(configuredCustomerCount);
		m_Security.init( TradingTimeSoFar, pBaseTime, pCurrentTime, m_fMeanInTheMoneySubmissionDelay );
		//TODO IMPORTANT
//		m_pSecurityFile.LoadSymbolToIdMap();
		
	}
	
	public	void getSymbol(long SecurityIndex,String szOutput, int outputLen){
		szOutput = m_pSecurityFile.createSymbol( SecurityIndex, outputLen );
	}
	
	public	EGenMoney getMinPrice(){
		return( m_Security.getMinPrice( ));
	}
	
	public	EGenMoney   getMaxPrice(){
		return( m_Security.getMaxPrice( ));
	}
	
	public EGenMoney   getCurrentPrMEESecurityice( long SecurityIndex ){
		return( m_Security.getCurrentPrice( SecurityIndex ));
	}
	
	public EGenMoney  getCurrentPrice( long SecurityIndex )
	{
	    return( m_Security.getCurrentPrice( SecurityIndex ));
	}

	public EGenMoney   getCurrentPrice( String  pSecuritySymbol ){
		return new EGenMoney(0);
//		return( m_Security.getCurrentPrice( m_pSecurityFile.getIndex( pSecuritySymbol )));//needs LoadSymbolToIdMap
	}
	
	public EGenMoney   CalculatePrice(String pSecuritySymbol, double fTime ){
		return new EGenMoney(0);
//		return( m_Security.CalculatePrice( m_pSecurityFile.getIndex( pSecuritySymbol ), fTime ));
	}
	
	public double   getSubmissionTime(String pSecuritySymbol, double fPendingTime, EGenMoney fLimitPrice, TradeType eTradeTypeID){
		return 0.00;
//		return( m_Security.getSubmissionTime( m_pSecurityFile.getIndex( pSecuritySymbol ), fPendingTime, fLimitPrice, eTradeTypeID ));
	}
	
	public double   getSubmissionTime(long SecurityIndex, double fPendingTime, EGenMoney fLimitPrice, TradeType eTradeTypeID){
		return( m_Security.getSubmissionTime( SecurityIndex, fPendingTime, fLimitPrice, eTradeTypeID ));
	}
	
	public double   getCompletionTime(long SecurityIndex,double fSubmissionTime, EGenMoney pCompletionPrice ){
		Object obj = m_Security.getCompletionTimeAndPrice( SecurityIndex, fSubmissionTime)[0];
		String str = obj.toString();
		return Double.valueOf(str).doubleValue();
	}
	
	public long m_iNumberOfSecurities;
	private double              m_fMeanInTheMoneySubmissionDelay;
    private MEESecurity        m_Security;
    private SecurityHandler      m_pSecurityFile;
}
