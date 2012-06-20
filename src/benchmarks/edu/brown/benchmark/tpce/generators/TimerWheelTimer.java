package edu.brown.benchmark.tpce.generators;

import java.lang.reflect.Method;

public class TimerWheelTimer {
    private Object expiryData;                      
    private Object expiryObject;                    
    private Method expiryFunction;  

    public TimerWheelTimer( Object expiryObject, Method expiryFunction, Object expiryData){
        this.expiryData = expiryData;
        this.expiryObject = expiryObject;
        this.expiryFunction = expiryFunction;
    }
    
    public Object getExpiryData(){
        return expiryData;
    }
    public Object getExpiryObject(){
        return expiryObject;
    }
    public Method getExpiryFunction(){
        return expiryFunction;
    }
}
