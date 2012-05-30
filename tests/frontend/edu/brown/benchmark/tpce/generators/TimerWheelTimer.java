package edu.brown.benchmark.tpce.generators;

import java.lang.reflect.Method;

public class TimerWheelTimer {
	Object m_pExpiryData;                      //The data to be passed back
    Object m_pExpiryObject;                    //The object on which to call the function
    Method m_pExpiryFunction;  //The function to call at expiration

    public TimerWheelTimer( Object pExpiryObject, Method pExpiryFunction, Object pExpiryData){
        m_pExpiryData = pExpiryData;
        m_pExpiryObject = pExpiryObject;
        m_pExpiryFunction = pExpiryFunction;
    }
}
