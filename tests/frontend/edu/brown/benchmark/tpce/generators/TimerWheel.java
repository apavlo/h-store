package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import edu.brown.benchmark.tpce.util.EGenDate;
import java.lang.reflect.*;

public class TimerWheel {
	
	private GregorianCalendar                           m_BaseTime;

	private WheelTime                          m_LastTime;
	private  WheelTime                          m_CurrentTime;
	private  WheelTime                          m_NextTime;
	private TWheelConfig                        m_WheelConfig;
	private ArrayList<LinkedList< TimerWheelTimer>>  m_TimerWheel;//[ ( Period * ( MsPerSecond / Resolution )) ];
	private  int                               m_NumberOfTimers;
	private int 								m_period;
	private int 								m_resolution;
	private Object								m_pExpiryData;
	private Object 								m_pExpiryObject;
	private Method 								m_pExpiryFunction;
	public  static final int  NO_OUTSTANDING_TIMERS = -1;

	 
	public TimerWheel(Object expiryData, Object expiryObject, Method pExpiryFunction, int Period, int Resolution){
		m_period = Period;
		m_resolution = Resolution;
		 m_BaseTime = new GregorianCalendar();
		 m_LastTime = new WheelTime( m_WheelConfig, 0, 0 );
		 m_CurrentTime = new WheelTime( m_WheelConfig, 0, 0 );
		 m_NextTime = new WheelTime( m_WheelConfig, TWheelConfig.MaxWheelCycles, ( Period * ( EGenDate.MsPerSecond / Resolution )) - 1 );
		 m_WheelConfig = new TWheelConfig(( Period * ( EGenDate.MsPerSecond / Resolution )), Resolution );
		 m_NumberOfTimers = 0;
		 m_TimerWheel = new ArrayList<LinkedList< TimerWheelTimer>>(Period * ( EGenDate.MsPerSecond / Resolution )) ;
		 m_pExpiryData = expiryData;
		 m_pExpiryObject = expiryObject;
		 m_pExpiryFunction = pExpiryFunction;
	 }
	   
	
/*	 
	 ~CTimerWheel()
	{
	    typename list< CTimerWheelTimer<T, T2>* >::iterator ExpiredTimer;

	    for( int ii=0; ii < ( Period * ( MsPerSecond / Resolution )); ii++ )
	    {
	        if( ! m_TimerWheel[ii].empty() )
	        {
	            ExpiredTimer = m_TimerWheel[ii].begin();
	            while( ExpiredTimer != m_TimerWheel[ii].end() )
	            {
	                delete *ExpiredTimer;
	                m_NumberOfTimers--;
	                ExpiredTimer++;
	            }
	            m_TimerWheel[ii].clear();
	        }
	    }
	}
*/
	 
	public boolean  Empty(){
	    return( m_NumberOfTimers == 0 ? true : false );
	}

	 
	public int  StartTimer( double Offset){
		GregorianCalendar                   Now = new GregorianCalendar();
	    WheelTime                  RequestedTime = new WheelTime( m_WheelConfig , m_BaseTime, Now, (int) (Offset * ( EGenDate.MsPerSecond / m_resolution )));
	    TimerWheelTimer    pNewTimer = new TimerWheelTimer( m_pExpiryObject, m_pExpiryFunction, m_pExpiryData );

	    //Update current wheel position
	    m_CurrentTime.Set( m_BaseTime, Now );

	    // Since the current time has been updated, we should make sure
	    // any outstanding timers have been processed before proceeding.
	    ExpiryProcessing();

	    m_TimerWheel.get(RequestedTime.Index()).add( pNewTimer );
	    m_NumberOfTimers++;
	    if( RequestedTime.m_Cycles == m_NextTime.m_Cycles ? (RequestedTime.m_Index < m_NextTime.m_Index) : ( RequestedTime.m_Cycles < m_NextTime.m_Cycles )){
	        m_NextTime = RequestedTime;
	    }

	    return( m_NextTime.Offset( m_CurrentTime ));
	}

	public int  ProcessExpiredTimers(){
		GregorianCalendar   Now = new GregorianCalendar();

	    // Update current wheel position
	    m_CurrentTime.Set( m_BaseTime, Now );

	    return( ExpiryProcessing() );
	}

	private int  ExpiryProcessing(){
	    while( m_LastTime.m_Cycles < m_CurrentTime.m_Cycles ? ( m_LastTime.m_Index < m_CurrentTime.m_Index ) : ( m_LastTime.m_Cycles < m_CurrentTime.m_Cycles ))
	    {
	        m_LastTime.Add(1);
	        if( ! m_TimerWheel.get( m_LastTime.Index()).isEmpty() )
	        {
	            ProcessTimerList( m_TimerWheel.get( m_LastTime.Index()) );
	        }
	    }
	    return( SetNextTime() );
	}

			 
	private void  ProcessTimerList( LinkedList<TimerWheelTimer> pList )
	{
		ListIterator<TimerWheelTimer>  ExpiredTimer = pList.listIterator();

	    while (ExpiredTimer.hasNext()){
	    	try{
	    		ExpiredTimer.next().m_pExpiryFunction.invoke(m_pExpiryObject, m_pExpiryData);
	    		ExpiredTimer.remove();
		        m_NumberOfTimers--;
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
	    	
	    }
	    pList.clear();
	}

	 
	private int  SetNextTime()
	{
	    if( 0 == m_NumberOfTimers )
	    {
	        m_NextTime.Set( TWheelConfig.MaxWheelCycles, ( m_period * ( EGenDate.MsPerSecond / m_resolution )) - 1 );
	        return( NO_OUTSTANDING_TIMERS );
	    }
	    else
	    {
	        m_NextTime = m_CurrentTime;
	        while( m_TimerWheel.get(m_NextTime.Index()).isEmpty() )
	        {
	            m_NextTime.Add(1);
	        }
	        return( m_NextTime.Offset( m_CurrentTime ));
	    }
	}

}
