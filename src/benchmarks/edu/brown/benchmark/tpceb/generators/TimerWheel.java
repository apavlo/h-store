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
    /*DEBUGGING: not sure this is the right data structure? In C++ TimerWheel. this is a list of arrays. May be why
     * our code has so many problems*/
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
        /*DEBUGGING: in java setting capacity/initial size of Arraylist does not allow you to get/set with index values
         * all index values must first be set to another value and then it can be changed. C++ allows a user to access/get/set
         * values that are not set but within given size/capacity I added the for loop below for debugging purposes*/
        for(int i = 0; i < period * ( EGenDate.MsPerSecond / resolution ); i++ ){
            timerWheel.add(null);
            
        }
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
        System.out.println("IN START TIMER");
        System.out.println("Is timer empty"+ timerWheel.isEmpty());
        currentTime.set( baseTime, Now );
        expiryProcessing();
        System.out.println("back in start timer out of expiry processing");
        
        /*DEBUGGING: 
         * previous code: timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
         * not valid in java when list here is set to be null originally (as I did in constructor)
         * add an if statement to check that the value exists and is null
         * then use call set, otherwise use add
         */
        
        //timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
        //changed to set
        if(timerWheel.get(RequestedTime.getIndex()) == null){
            System.out.println("This was true");
            LinkedList<TimerWheelTimer> newList = new LinkedList<TimerWheelTimer>();
            newList.add(pNewTimer);
          //  timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
            timerWheel.set(RequestedTime.getIndex(), newList);
        }
        else{
            System.out.println("Second was true");
            timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
        }
        numberOfTimers++;
        if( RequestedTime.getCycles() == nextTime.getCycles() ? (RequestedTime.getIndex() < nextTime.getIndex()) : ( RequestedTime.getCycles() < nextTime.getCycles() )){
            nextTime = RequestedTime;
        }
        System.out.println("LENGTH" + timerWheel.size());

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
            System.out.println("last time index before" + lastTime.getIndex());
            lastTime.add(1);
       
            System.out.println(timerWheel.size());
        
            System.out.println("last time index up" + lastTime.getIndex());
            System.out.println("in loop");
            /*DEBUGGING: first make sure timer wheel isn't empty. Otherwise 3rd if condition will break same C++/Java reasons
             * then check that the value at the index isn't null - valid java code
             * 
             * */
            if(! timerWheel.isEmpty()){ //DEBUGGING: added if
                if( !( timerWheel.get(lastTime.getIndex()) == null)){ //DEBUGGING: added if
                    System.out.println("This worked");
                if( ! timerWheel.get( lastTime.getIndex()).isEmpty() ){ //original statement
                    System.out.println("adding to timer list");
                    processTimerList( timerWheel.get( lastTime.getIndex()) );
                }
                }
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
            /*DEBUGGING: added in the if statement b/c java will not recognize condition in the while loop if it is null*/
            if( !(timerWheel.get(nextTime.getIndex()) == null )){
              
                // the while loop breaks when you increase the next time index. most likely b/c timerWheel is not initialized
                // this is valid in c++ when only size/capacity is set. in Java must have values (even null filled in)
                while( timerWheel.get(nextTime.getIndex()).isEmpty() ){
                    nextTime.add(1);
                   
                }
            }
            return( nextTime.offset( currentTime ));
        }
    }

}
