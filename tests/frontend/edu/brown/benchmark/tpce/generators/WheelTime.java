package edu.brown.benchmark.tpce.generators;

import java.util.GregorianCalendar;

public class WheelTime {
	private TWheelConfig    m_pWheelConfig; //Pointer to configuration info for the wheel
    int           m_Cycles;       //Number of completed cycles so far
    int           m_Index;        //Index into the current cycle

	public WheelTime( TWheelConfig pWheelConfig ){
		m_pWheelConfig = pWheelConfig;
	    m_Cycles = 0;
	    m_Index = 0;
	}
	
	public WheelTime( TWheelConfig pWheelConfig, int cycles, int index ){
		m_pWheelConfig = pWheelConfig;
	    m_Cycles = cycles;
	    m_Index = index;
	}

	public WheelTime( TWheelConfig pWheelConfig, GregorianCalendar Base, GregorianCalendar Now, int offset ){
	    m_pWheelConfig = pWheelConfig ;
	    Set( Base , Now );
	    Add( offset );
	}
	public int Cycles() { return m_Cycles; };
    public int Index() { return m_Index; };

	public void  Add( int Interval ){
	    //DJ - should throw error if Interval >= m_pWheelConfig.WheelSize?
	    m_Cycles += Interval / m_pWheelConfig.WheelSize;
	    m_Index += Interval % m_pWheelConfig.WheelSize;
	    if( m_Index >= m_pWheelConfig.WheelSize ){
	        //Handle wrapping in the wheel - assume we don't allow multi-cycle intervals
	        m_Cycles++;
	        m_Index -= m_pWheelConfig.WheelSize;
	    }
	}
	
	public int  Offset( final WheelTime Time ){
	    int   Interval;
	
	    Interval = ( m_Cycles - Time.m_Cycles ) * m_pWheelConfig.WheelSize;
	    Interval += ( m_Index - Time.m_Index );
	    return( Interval );
	}
	
	public void  Set( int cycles, int index ){
	    m_Cycles = cycles;
	    m_Index = index;    //DJ - should throw error if Index >= m_pWheelConfig.WheelSize
	}
	
	// Set is overloaded. This version is used by the timer wheel.
	public void  Set( GregorianCalendar Base, GregorianCalendar Now ){
	    int       offset; //offset from BaseTime in milliseconds
	
	    //DJ - If Now < Base, then we should probably throw an exception
	
	    offset = (int)(Now.getTimeInMillis() - Base.getTimeInMillis()) / m_pWheelConfig.WheelResolution; // convert based on wheel resolution
	    m_Cycles = offset / m_pWheelConfig.WheelSize;
	    m_Index = offset % m_pWheelConfig.WheelSize;
	}
	
	// Set is overloaded. This version is used by the event wheel.
	/*	public void  Set( GregorianCalendar pBase, GregorianCalendar pNow )
	{
	    int       offset; //offset from BaseTime in milliseconds
	
	    //DJ - If Now < Base, then we should probably throw an exception
	
	    offset = (int)(pNow.getTimeInMillis() - pBase.getTimeInMillis()) / m_pWheelConfig.WheelResolution; // convert based on wheel resolution
	    m_Cycles = offset / m_pWheelConfig.WheelSize;
	    m_Index = offset % m_pWheelConfig.WheelSize;
	}*/
	/*
	bool  operator <(const WheelTime& Time)
	{
	    return ( m_Cycles == Time.m_Cycles ) ? ( m_Index < Time.m_Index ) : ( m_Cycles < Time.m_Cycles );
	}
	
	WheelTime&  operator = (const WheelTime& Time)
	{
	    m_pWheelConfig = Time.m_pWheelConfig;
	    m_Cycles = Time.m_Cycles;
	    m_Index = Time.m_Index;
	
	    return *this;
	}
	
	WheelTime&  operator += ( const int& Interval )
	{
	    Add( Interval );
	    return *this;
	}
	
	WheelTime  operator ++ ( int )
	{
	    Add( 1 );
	    return *this;
	}*/
}

class TWheelConfig{
	public static final int MaxWheelCycles = 999999999;
	public int   WheelSize;          // Total size of the wheel (based on the period and resolution)
	public int   WheelResolution;    // Expressed in milliseconds

    TWheelConfig( int Size, int Resolution ){
    	WheelSize = Size;
    	WheelResolution = Resolution;
    }
}
