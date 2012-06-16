package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import edu.brown.benchmark.tpce.util.EGenDate;
import java.lang.reflect.*;

public class TimerWheel {
    
    private GregorianCalendar                           baseTime;

    private WheelTime                                      lastTime;
    private WheelTime                                      currentTime;
    private WheelTime                                      nextTime;
    private TWheelConfig                                wheelConfig;
    private ArrayList<LinkedList< TimerWheelTimer>>      timerWheel;
    private int                                           numberOfTimers;
    private int                                         period;
    private int                                         resolution;
    private Object                                        expiryData;
    private Object                                         expiryObject;
    private Method                                         expiryFunction;
    public  static final int                              NO_OUTSTANDING_TIMERS = -1;

     
    public TimerWheel(Object expiryData, Object expiryObject, Method expiryFunction, int period, int resolution){
        wheelConfig = new TWheelConfig(( period * ( EGenDate.MsPerSecond / resolution )), resolution );
        this.period = period;
        this.resolution = resolution;
        baseTime = new GregorianCalendar();
        lastTime = new WheelTime( wheelConfig, 0, 0 );
        currentTime = new WheelTime( wheelConfig, 0, 0 );
        nextTime = new WheelTime( wheelConfig, TWheelConfig.MaxWheelCycles, ( period * ( EGenDate.MsPerSecond / resolution )) - 1 );
        numberOfTimers = 0;
        timerWheel = new ArrayList<LinkedList< TimerWheelTimer>>(period * ( EGenDate.MsPerSecond / resolution )) ;
        this.expiryData = expiryData;
        this.expiryObject = expiryObject;
        this.expiryFunction = expiryFunction;
     }
     
    public boolean  empty(){
        return( numberOfTimers == 0 ? true : false );
    }

     
    public int  startTimer( double Offset){
        GregorianCalendar                   Now = new GregorianCalendar();
        WheelTime                  RequestedTime = new WheelTime( wheelConfig , baseTime, Now, (int) (Offset * ( EGenDate.MsPerSecond / resolution )));
        TimerWheelTimer    pNewTimer = new TimerWheelTimer( expiryObject, expiryFunction, expiryData );

        currentTime.set( baseTime, Now );
        expiryProcessing();

        timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
        numberOfTimers++;
        if( RequestedTime.getCycles() == nextTime.getCycles() ? (RequestedTime.getIndex() < nextTime.getIndex()) : ( RequestedTime.getCycles() < nextTime.getCycles() )){
            nextTime = RequestedTime;
        }

        return( nextTime.offset( currentTime ));
    }

    public int  processExpiredTimers(){
        GregorianCalendar   Now = new GregorianCalendar();

        currentTime.set( baseTime, Now );

        return( expiryProcessing() );
    }

    private int  expiryProcessing(){
        while( lastTime.getCycles() < currentTime.getCycles() ? ( lastTime.getIndex() < currentTime.getIndex() ) : ( lastTime.getCycles() < currentTime.getCycles() )){
            lastTime.add(1);
            if( ! timerWheel.get( lastTime.getIndex()).isEmpty() ){
                processTimerList( timerWheel.get( lastTime.getIndex()) );
            }
        }
        return( setNextTime() );
    }

             
    private void  processTimerList( LinkedList<TimerWheelTimer> pList ){
        ListIterator<TimerWheelTimer>  ExpiredTimer = pList.listIterator();

        while (ExpiredTimer.hasNext()){
            try{
                ExpiredTimer.next().getExpiryFunction().invoke(expiryObject, expiryData);
                ExpiredTimer.remove();
                numberOfTimers--;
            }catch(Exception e){
                e.printStackTrace();
            }
            
        }
        pList.clear();
    }

     
    private int  setNextTime(){
        if( 0 == numberOfTimers ){
            nextTime.set( TWheelConfig.MaxWheelCycles, ( period * ( EGenDate.MsPerSecond / resolution )) - 1 );
            return( NO_OUTSTANDING_TIMERS );
        }
        else{
            nextTime = currentTime;
            while( timerWheel.get(nextTime.getIndex()).isEmpty() ){
                nextTime.add(1);
            }
            return( nextTime.offset( currentTime ));
        }
    }

}
