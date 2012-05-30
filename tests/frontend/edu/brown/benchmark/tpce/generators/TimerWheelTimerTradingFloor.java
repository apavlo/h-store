package edu.brown.benchmark.tpce.generators;

import java.lang.reflect.Method;

/*
public class TimerWheelTimerTradingFloor {

	public TTradeRequest  m_pExpiryData;                      //The data to be passed back
	public MEETradingFloor m_pExpiryObject;                    //The object on which to call the function

	public TimerWheelTimerTradingFloor( MEETradingFloor pExpiryObject, TTradeRequest pExpiryData ){
		m_pExpiryData = pExpiryData;
	    m_pExpiryObject = pExpiryObject;
	}
}
*/

public class TimerWheelTimerTradingFloor {

    Object m_pExpiryData;                      //The data to be passed back
    Object m_pExpiryObject;                    //The object on which to call the function
    Method m_pExpiryFunction;  //The function to call at expiration

    public TimerWheelTimerTradingFloor( Object pExpiryObject, Method pExpiryFunction, Object pExpiryData){
        m_pExpiryData = pExpiryData;
        m_pExpiryObject = pExpiryObject;
        m_pExpiryFunction = pExpiryFunction;
    }
}
