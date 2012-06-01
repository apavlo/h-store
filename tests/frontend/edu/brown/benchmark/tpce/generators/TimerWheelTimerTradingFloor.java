package edu.brown.benchmark.tpce.generators;

import java.lang.reflect.Method;

public class TimerWheelTimerTradingFloor {

    private Object m_pExpiryData;                      
    private Object m_pExpiryObject;                
    private Method m_pExpiryFunction;  

    public TimerWheelTimerTradingFloor( Object pExpiryObject, Method pExpiryFunction, Object pExpiryData){
        m_pExpiryData = pExpiryData;
        m_pExpiryObject = pExpiryObject;
        m_pExpiryFunction = pExpiryFunction;
    }
}
