package edu.brown.benchmark.tpce.generators;

import java.util.GregorianCalendar;

public class WheelTime {
    private TWheelConfig    wheelConfig; 
    private int             cycles;      
    private int             index;        

    public WheelTime( TWheelConfig pWheelConfig ){
        wheelConfig = pWheelConfig;
        cycles = 0;
        index = 0;
    }
    
    public WheelTime( TWheelConfig pWheelConfig, int cycles, int index ){
        wheelConfig = pWheelConfig;
        this.cycles = cycles;
        this.index = index;
    }

    public WheelTime( TWheelConfig pWheelConfig, GregorianCalendar base, GregorianCalendar now, int offset ){
        wheelConfig = pWheelConfig ;
        set( base , now );
        add( offset );
    }
    public int getCycles(){ 
        return cycles; 
    }
    public int getIndex(){ 
        return index; 
    }

    public void  add( int interval ){

        cycles += interval / wheelConfig.getWheelSize();
        index += interval % wheelConfig.getWheelSize();
        if( index >= wheelConfig.getWheelSize() ){
          
            cycles++;
            index -= wheelConfig.getWheelSize();
        }
    }
    
    public int  offset( final WheelTime Time ){
        int   interval;
    
        interval = ( cycles - Time.cycles ) * wheelConfig.getWheelSize();
        interval += ( index - Time.index );
        return( interval );
    }
    
    public void  set( int cycles, int index ){
        this.cycles = cycles;
        this.index = index;   
    }
    
    public void  set( GregorianCalendar base, GregorianCalendar now ){
        int       offset; 
    
        offset = (int)(now.getTimeInMillis() - base.getTimeInMillis()) / wheelConfig.getWheelSize(); // convert based on wheel resolution
        cycles = offset / wheelConfig.getWheelSize();
        index = offset % wheelConfig.getWheelSize();
    }

}

class TWheelConfig{
    public static final int MaxWheelCycles = 999999999;
    private int   WheelSize;          
    private int   WheelResolution;    

    TWheelConfig( int Size, int Resolution ){
        WheelSize = Size;
        WheelResolution = Resolution;
    }
    public int getWheelSize(){
        return WheelSize;
    }
    public int getWheelResolution(){
        return WheelResolution;
    }
    public void setWheelSize(int WheelSize){
        this.WheelSize = WheelSize;
    }
    public void setWheelResolution(int WheelResolution){
        this.WheelResolution = WheelResolution;
    }
}
