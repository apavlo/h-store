package edu.brown.benchmark.tpceb.generators;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import edu.brown.benchmark.tpceb.util.EGenDate;
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
        System.out.println("in timer wheel");
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
        System.out.println("expiry data" + expiryData);
        System.out.println("expiry object" + expiryObject);
        System.out.println("expiry function" + expiryFunction);
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
       // System.out.println(baseTime);// + " " + Now);
        System.out.println("going to expiry processing");
        return( expiryProcessing() );
    }

    private int  expiryProcessing(){
        System.out.println("In expiry processing");
        System.out.println("lastTime cycles" + lastTime.getCycles() );
        System.out.println("currentTime cycles" + currentTime.getCycles() );
        System.out.println("lastTime index" + lastTime.getIndex() );
        System.out.println("currentTime index" + currentTime.getIndex() );
        while( lastTime.getCycles() == currentTime.getCycles() ? ( lastTime.getIndex() < currentTime.getIndex() ) : ( lastTime.getCycles() < currentTime.getCycles() )){
      // while(lastTime < currentTime){
            System.out.println("last time index before" + lastTime.getIndex());
        lastTime.add(1);
        if(timerWheel == null){
            System.out.println("null tw");
        }
        System.out.println(timerWheel.size());
        System.out.println("last time index up" + lastTime.getIndex());
            System.out.println("in loop");
           // System.out.println("timerWheel etc" + timerWheel.get(lastTime.getIndex()));
           
            if( ! timerWheel.get( lastTime.getIndex()).isEmpty() ){
                System.out.println("adding to timer list");
                processTimerList( timerWheel.get( lastTime.getIndex()) );
            }
        }
        return( setNextTime() );
    }

             
    private void  processTimerList( LinkedList<TimerWheelTimer> pList ){
        ListIterator<TimerWheelTimer>  ExpiredTimer = pList.listIterator();

        while (ExpiredTimer.hasNext()){
            try{
                System.out.println("In loop going through expired timer to get expiry functions" + ExpiredTimer.next().getExpiryFunction() );
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
        System.out.println("in set next time");
        if( 0 == numberOfTimers ){
            System.out.println("number of timers was 0");
            nextTime.set( TWheelConfig.MaxWheelCycles, ( period * ( EGenDate.MsPerSecond / resolution )) - 1 );
            System.out.println("new next time" + nextTime.getCycles());
            return( NO_OUTSTANDING_TIMERS );
        }
        else{
            
            nextTime = currentTime;
            while( timerWheel.get(nextTime.getIndex()).isEmpty() ){
                System.out.println("adding one to time");
                nextTime.add(1);
            }
            return( nextTime.offset( currentTime ));
        }
    }

}
