package edu.brown.benchmark.tpceb.generators;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
    //private ArrayList<LinkedList< TimerWheelTimer>>      timerWheel;
    private ArrayList<BlockingQueue< TimerWheelTimer>>      timerWheel;
    private int                                           numberOfTimers;
    private int                                         period;
    private int                                         resolution;
    private Object                                        expiryData;
    private Object                                         expiryObject;
    private Method                                         expiryFunction;
    public  static final int                              NO_OUTSTANDING_TIMERS = -1;

     
    public TimerWheel(Object expiryData, Object expiryObject, Method expiryFunction, int period, int resolution){
        //System.out.println("in timer wheel");
        wheelConfig = new TWheelConfig(( period * ( EGenDate.MsPerSecond / resolution )), resolution );
        this.period = period;
        this.resolution = resolution;
        baseTime = new GregorianCalendar();
        lastTime = new WheelTime( wheelConfig, 0, 0 );
        currentTime = new WheelTime( wheelConfig, 0, 0 );
        nextTime = new WheelTime( wheelConfig, TWheelConfig.MaxWheelCycles, ( period * ( EGenDate.MsPerSecond / resolution )) - 1 );
        numberOfTimers = 0;
       // timerWheel = new ArrayList<LinkedList< TimerWheelTimer>>(period * ( EGenDate.MsPerSecond / resolution )) ;
        timerWheel = new ArrayList<BlockingQueue< TimerWheelTimer>>(period * ( EGenDate.MsPerSecond / resolution )) ;
        /*DEBUGGING: in java setting capacity/initial size of Arraylist does not allow you to get/set with index values
         * all index values must first be set to another value and then it can be changed. C++ allows a user to access/get/set
         * values that are not set but within given size/capacity I added the for loop below for debugging purposes*/
        for(int i = 0; i < period * ( EGenDate.MsPerSecond / resolution ); i++ ){
          //  timerWheel.add(new LinkedList<TimerWheelTimer>());
            timerWheel.add(new LinkedBlockingQueue<TimerWheelTimer>());
            
        }
        this.expiryData = expiryData;
        this.expiryObject = expiryObject;
        this.expiryFunction = expiryFunction;
       // System.out.println("expiry data first" + expiryData);
       // System.out.println("expiry object first" + expiryObject);
        //System.out.println("expiry function first" + expiryFunction);
     }
     
    public boolean  empty(){
        return( numberOfTimers == 0 ? true : false );
    }

    /*added*/
    public int  startTimer( double Offset, Object expiryObject, Object expiryData ){
       // System.out.println("IN START TIMER");
        GregorianCalendar                   Now = new GregorianCalendar();
        
        WheelTime                  RequestedTime = new WheelTime( wheelConfig , baseTime, Now, (int) (Offset * ( EGenDate.MsPerSecond / resolution )));
        TimerWheelTimer    pNewTimer = new TimerWheelTimer( expiryObject, expiryFunction, expiryData );
       // System.out.println("Original: " + RequestedTime.getIndex());
        currentTime.set( baseTime, Now );
       // System.out.println("Original currentTime: " + currentTime.getIndex());
        expiryProcessing();
        
        this.expiryData = expiryData;
        this.expiryObject = expiryObject;
       // System.out.println("ADDING TO:" + RequestedTime.getIndex());
        /*DEBUGGING: 
         * previous code: timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
         * not valid in java when list here is set to be null originally (as I did in constructor)
         * add an if statement to check that the value exists and is null
         * then use call set, otherwise use add
         */
        
        //timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
        //changed to set
        if(timerWheel.get(RequestedTime.getIndex()) == null){
          //  System.out.println("This was true");
            //LinkedList<TimerWheelTimer> newList = new LinkedList<TimerWheelTimer>();
            BlockingQueue<TimerWheelTimer> newList = new LinkedBlockingQueue<TimerWheelTimer>();
            newList.add(pNewTimer);
            
          //  timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
            timerWheel.set(RequestedTime.getIndex(), newList);
        }
        else{
            timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
           // System.out.println("NOW: timerwheel size at " + RequestedTime.getIndex() + " " + timerWheel.get(RequestedTime.getIndex()).size());
        }
        numberOfTimers++;
       // System.out.println("Number of timers" + numberOfTimers);
        if( RequestedTime.getCycles() == nextTime.getCycles() ? (RequestedTime.getIndex() < nextTime.getIndex()) : ( RequestedTime.getCycles() < nextTime.getCycles() )){
          //  System.out.println("new next time");
            nextTime = RequestedTime;
        }
       // System.out.println("LENGTH" + timerWheel.size());

        return( nextTime.offset( currentTime ));
    }
    
    
    
    
    public int  startTimer( double Offset){
        GregorianCalendar                   Now = new GregorianCalendar();
        WheelTime                  RequestedTime = new WheelTime( wheelConfig , baseTime, Now, (int) (Offset * ( EGenDate.MsPerSecond / resolution )));
        TimerWheelTimer    pNewTimer = new TimerWheelTimer( expiryObject, expiryFunction, expiryData );
      //  System.out.println("IN START TIMER");
       // System.out.println("Is timer empty"+ timerWheel.isEmpty());
        currentTime.set( baseTime, Now );
        expiryProcessing();
       /// System.out.println("back in start timer out of expiry processing");
        
        /*DEBUGGING: 
         * previous code: timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
         * not valid in java when list here is set to be null originally (as I did in constructor)
         * add an if statement to check that the value exists and is null
         * then use call set, otherwise use add
         */
        
        //timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
        //changed to set
        if(timerWheel.get(RequestedTime.getIndex()) == null){
         //   System.out.println("This was true");
            //LinkedList<TimerWheelTimer> newList = new LinkedList<TimerWheelTimer>();
            BlockingQueue<TimerWheelTimer> newList = new LinkedBlockingQueue<TimerWheelTimer>();
            newList.add(pNewTimer);
            
          //  timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
            timerWheel.set(RequestedTime.getIndex(), newList);
        }
        else{
           // System.out.println("Second was true");
           // System.out.println("timer wheel length: "+ timerWheel.size());
           // System.out.println("timerwheel size at " + RequestedTime.getIndex() + " " + timerWheel.get(RequestedTime.getIndex()).size());
            timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
           // System.out.println("NOW: timerwheel size at " + RequestedTime.getIndex() + " " + timerWheel.get(RequestedTime.getIndex()).size());
        }
        numberOfTimers++;
        if( RequestedTime.getCycles() == nextTime.getCycles() ? (RequestedTime.getIndex() < nextTime.getIndex()) : ( RequestedTime.getCycles() < nextTime.getCycles() )){
            nextTime = RequestedTime;
        }
       // System.out.println("LENGTH" + timerWheel.size());

        return( nextTime.offset( currentTime ));
    }

    public int  processExpiredTimers(){
        GregorianCalendar   Now = new GregorianCalendar();

        currentTime.set( baseTime, Now );

        return( expiryProcessing() );
    }

    private int  expiryProcessing(){
       // System.out.println("in expiry processing current time cycles" + currentTime.getCycles());
       // System.out.println("in expiry processing current time index" + currentTime.getIndex());
       // System.out.println("in expiry processing last time cycles" + lastTime.getCycles());
       // System.out.println("in expiry processing last time index" + lastTime.getIndex());
        while( lastTime.getCycles() == currentTime.getCycles() ? ( lastTime.getIndex() < currentTime.getIndex() ) : ( lastTime.getCycles() < currentTime.getCycles() )){
          //  System.out.println("last time index before" + lastTime.getIndex());
            lastTime.add(1);
       
        
            /*DEBUGGING: first make sure timer wheel isn't empty. Otherwise 3rd if condition will break same C++/Java reasons
             * then check that the value at the index isn't null - valid java code
             * 
             * */
        //    System.out.println("index trying to access: " + lastTime.getIndex());
         //   System.out.println("size at that index" + timerWheel.get(lastTime.getIndex()).size() );
            if(! timerWheel.isEmpty()){ //DEBUGGING: added if
                if( !( timerWheel.get(lastTime.getIndex()) == null)){ //DEBUGGING: added if
                  //  System.out.println("This worked");
                  //  System.out.println("value was " + timerWheel.get(lastTime.getIndex()) );
                if( ! timerWheel.get( lastTime.getIndex()).isEmpty() ){ //original statement
                  //  System.out.println("adding to timer list");
                  //  System.out.println("print" + timerWheel.get(lastTime.getIndex()));
                    processTimerList( timerWheel.get( lastTime.getIndex()) );
                }
                }
            }
        }
        
        return( setNextTime() );
    }

             
    private void  processTimerList(BlockingQueue<TimerWheelTimer> pList){// LinkedList<TimerWheelTimer> pList ){
        //ListIterator<TimerWheelTimer>  ExpiredTimer = pList.listIterator(); 
        Iterator<TimerWheelTimer>  ExpiredTimer = pList.iterator();
        //System.out.println("IN PROCESS TIMER LIST");
      //  while (ExpiredTimer.hasNext()){
        while(pList.peek() != null) {   
        try{
                //System.out.println("In loop going through expired timer to get expiry functions" + ExpiredTimer.next().getExpiryFunction() );
               // System.out.println("IN LOOP!!!!!!! FINALLY");
               // System.out.println(ExpiredTimer.next());
                
               // if( ExpiredTimer.next().getExpiryFunction() == null){
                //    System.out.println("WAS NULL");
               // }
                
                TimerWheelTimer value = pList.poll();
                if(value !=null){
                   // System.out.println("Got next");
                
               //     System.out.println("DATA2: " + expiryData);
                    //expiryData = new TTradeRequest();
                  //  System.out.println("OBJECT1: " + value.getExpiryObject());
                   // System.out.println("OBJECT1: " + expiryObject);
                   // System.out.println( "FUNCTION1:" + value.getExpiryFunction());
                   // System.out.println( "FUNCTION1:" + expiryFunction);
                    value.getExpiryFunction().invoke(expiryObject, expiryData);
                    
                  //  System.out.println("invoked Expiry function");
                    numberOfTimers--;
               // ExpiredTimer.remove();
                }
                
            }catch(Exception e){
            //    System.out.println("e" + e);
            //    System.out.println("e message 1" +e.getLocalizedMessage());
             //   System.out.println("e message 2" + e.getMessage());
              ///  System.out.println("e message 1" +e.getCause() );
                //System.out.println("e message 1" +e.getLocalizedMessage() );
                e.printStackTrace();
            }
            
        }
        pList.clear();
    }

     
    private int  setNextTime(){
        //System.out.println("in set next time");
        if( 0 == numberOfTimers ){
          //  System.out.println("number of timers was 0");
            nextTime.set( TWheelConfig.MaxWheelCycles, ( period * ( EGenDate.MsPerSecond / resolution )) - 1 );
           // System.out.println("new next time" + nextTime.getCycles());
            return( NO_OUTSTANDING_TIMERS );
        }
        else{
            
            nextTime = currentTime;
          //  System.out.println("nextTime.getIndex()" + nextTime.getIndex());
         //   System.out.println("timerWheel at index" + timerWheel.get(nextTime.getIndex()) );
            /*DEBUGGING: added in the if statement b/c java will not recognize condition in the while loop if it is null*/
            if( !(timerWheel.get(nextTime.getIndex()) == null )){
              
                // the while loop breaks when you increase the next time index. most likely b/c timerWheel is not initialized
                // this is valid in c++ when only size/capacity is set. in Java must have values (even null filled in)
                while( timerWheel.get(nextTime.getIndex()).isEmpty() ){
                    nextTime.add(1);
                   
                }
            }
            //System.out.println("returning");
            return( nextTime.offset( currentTime ));
        }
    }

}

/*package edu.brown.benchmark.tpceb.generators;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
    //private ArrayList<LinkedList< TimerWheelTimer>>      timerWheel;
  /*  private ArrayList<BlockingQueue< TimerWheelTimer>>      timerWheel;
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
       // timerWheel = new ArrayList<LinkedList< TimerWheelTimer>>(period * ( EGenDate.MsPerSecond / resolution )) ;
        timerWheel = new ArrayList<BlockingQueue< TimerWheelTimer>>(period * ( EGenDate.MsPerSecond / resolution )) ;
        /*DEBUGGING: in java setting capacity/initial size of Arraylist does not allow you to get/set with index values
         * all index values must first be set to another value and then it can be changed. C++ allows a user to access/get/set
         * values that are not set but within given size/capacity I added the for loop below for debugging purposes*/
  /*      for(int i = 0; i < period * ( EGenDate.MsPerSecond / resolution ); i++ ){
          //  timerWheel.add(new LinkedList<TimerWheelTimer>());
            timerWheel.add(new LinkedBlockingQueue<TimerWheelTimer>());
            
        }
        this.expiryData = expiryData;
        this.expiryObject = expiryObject;
        this.expiryFunction = expiryFunction;
        System.out.println("expiry data first" + expiryData);
        System.out.println("expiry object first" + expiryObject);
        System.out.println("expiry function first" + expiryFunction);
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
  /*      if(timerWheel.get(RequestedTime.getIndex()) == null){
            System.out.println("This was true");
            //LinkedList<TimerWheelTimer> newList = new LinkedList<TimerWheelTimer>();
            BlockingQueue<TimerWheelTimer> newList = new LinkedBlockingQueue<TimerWheelTimer>();
            newList.add(pNewTimer);
            
          //  timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
            timerWheel.set(RequestedTime.getIndex(), newList);
        }
        else{
            System.out.println("Second was true");
            System.out.println("timer wheel length: "+ timerWheel.size());
            System.out.println("timerwheel size at " + RequestedTime.getIndex() + " " + timerWheel.get(RequestedTime.getIndex()).size());
            timerWheel.get(RequestedTime.getIndex()).add( pNewTimer );
            System.out.println("NOW: timerwheel size at " + RequestedTime.getIndex() + " " + timerWheel.get(RequestedTime.getIndex()).size());
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
       
            System.out.println("timerWheel size in expiry processing " + timerWheel.size());
        
            System.out.println("last time index up" + lastTime.getIndex());
            System.out.println("in loop");
            /*DEBUGGING: first make sure timer wheel isn't empty. Otherwise 3rd if condition will break same C++/Java reasons
             * then check that the value at the index isn't null - valid java code
             * 
             * */
   /*         System.out.println("index trying to access: " + lastTime.getIndex());
            System.out.println("size at that index" + timerWheel.get(lastTime.getIndex()).size() );
            if(! timerWheel.isEmpty()){ //DEBUGGING: added if
                if( !( timerWheel.get(lastTime.getIndex()) == null)){ //DEBUGGING: added if
                    System.out.println("This worked");
                if( ! timerWheel.get( lastTime.getIndex()).isEmpty() ){ //original statement
                    System.out.println("adding to timer list");
                    System.out.println("print" + timerWheel.get(lastTime.getIndex()));
                    processTimerList( timerWheel.get( lastTime.getIndex()) );
                }
                }
            }
        }
        return( setNextTime() );
    }

             
    private void  processTimerList(BlockingQueue<TimerWheelTimer> pList){// LinkedList<TimerWheelTimer> pList ){
        //ListIterator<TimerWheelTimer>  ExpiredTimer = pList.listIterator(); 
        Iterator<TimerWheelTimer>  ExpiredTimer = pList.iterator();
        System.out.println("IN PROCESS TIMER LIST");
      //  while (ExpiredTimer.hasNext()){
        while(pList.peek() != null) {   
        try{
                //System.out.println("In loop going through expired timer to get expiry functions" + ExpiredTimer.next().getExpiryFunction() );
                System.out.println("IN LOOP!!!!!!! FINALLY");
               // System.out.println(ExpiredTimer.next());
                
               // if( ExpiredTimer.next().getExpiryFunction() == null){
                //    System.out.println("WAS NULL");
               // }
                
                TimerWheelTimer value = pList.poll();
                if(value !=null){
                    System.out.println("Got next");
                ////System.out.println("Getting Function:" + ExpiredTimer.next().getExpiryFunction());
                //ExpiredTimer.next().getExpiryFunction().invoke(expiryObject, expiryData);
                    if(expiryObject == null){
                        System.out.println("Expriy object was null");
                        
                    }
                    if(expiryData == null){
                        System.out.println("Expiry Data was null");
                    }
                    System.out.println("DATA1: " + value.getExpiryData());
                    System.out.println("DATA2: " + expiryData);
                    expiryData = new TTradeRequest();
                    System.out.println("OBJECT1: " + value.getExpiryObject());
                    System.out.println("OBJECT1: " + expiryObject);
                    System.out.println( "FUNCTION1:" + value.getExpiryFunction());
                    System.out.println( "FUNCTION1:" + expiryFunction);
                    value.getExpiryFunction().invoke(expiryObject, expiryData);
                    
                    System.out.println("invoked Expiry function");
                    numberOfTimers--;
               // ExpiredTimer.remove();
                }
                
            }catch(Exception e){
                System.out.println("e" + e);
                System.out.println("e message 1" +e.getLocalizedMessage());
                System.out.println("e message 2" + e.getMessage());
                System.out.println("e message 1" +e.getCause() );
                //System.out.println("e message 1" +e.getLocalizedMessage() );
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
     /*   else{
            
            nextTime = currentTime;
            /*DEBUGGING: added in the if statement b/c java will not recognize condition in the while loop if it is null*/
         //   if( !(timerWheel.get(nextTime.getIndex()) == null )){
              
                // the while loop breaks when you increase the next time index. most likely b/c timerWheel is not initialized
                // this is valid in c++ when only size/capacity is set. in Java must have values (even null filled in)
         /*       while( timerWheel.get(nextTime.getIndex()).isEmpty() ){
                    nextTime.add(1);
                   
                }
            }
            return( nextTime.offset( currentTime ));
        }
    }

}
*/
