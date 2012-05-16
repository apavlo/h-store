package edu.brown.benchmark.tpce.generators;

import java.util.Date;

public class MEEPriceBoard {
/*
	    // Mean delay between Pending and Submission times
	    // for an immediatelly triggered (in-the-money) limit order.
	    //
	private double              m_fMeanInTheMoneySubmissionDelay;
	private MEESecurity        m_Security;
	private SecurityHandler      m_pSecurityFile;

	
	public long              m_iNumberOfSecurities;

	public MEEPriceBoard( int           TradingTimeSoFar,
			Date baseTime, Date currentTime,
			SecurityHandler pSecurityFile, long iActiveCustomerCount
	                    ){
		m_fMeanInTheMoneySubmissionDelay = 1.0;
		m_Security = new MEESecurity();
		m_pSecurityFile = pSecurityFile;
		m_iNumberOfSecurities = SecurityHandler.getSecurityNum(iActiveCustomerCount);
		m_Security.init( TradingTimeSoFar, baseTime, currentTime, m_fMeanInTheMoneySubmissionDelay );
		m_pSecurityFile.LoadSymbolToIdMap();
	}
	   
	public void    GetSymbol(long  SecurityIndex,
	                        char[]   szOutput,       // output buffer
	                        size_t  iOutputLen);    // size of the output buffer (including null));

	public Money  GetMinPrice();

	public Money  GetMaxPrice();

	public Money  GetCurrentPrice( TIdent SecurityIndex );
	public Money  GetCurrentPrice( char* pSecuritySymbol );

	public Money  CalculatePrice( char* pSecuritySymbol, double fTime );

	public double  GetSubmissionTime(
	                                char*           pSecuritySymbol,
	                                double          fPendingTime,
	                                CMoney          fLimitPrice,
	                                eTradeTypeID    TradeType
	                                );
	public double  GetSubmissionTime(
	                                TIdent          SecurityIndex,
	                                double          fPendingTime,
	                                CMoney          fLimitPrice,
	                                eTradeTypeID    TradeType
	                                );
	public double  GetCompletionTime(
	                                TIdent      SecurityIndex,
	                                double      fSubmissionTime,
	                                CMoney*     pCompletionPrice    // output param
	                            );*/
}
