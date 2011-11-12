package edu.brown.logging;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

public class FastLoggingEvent extends LoggingEvent {
    private static final long serialVersionUID = -5595734504055550617L;
    
    private final LoggingEvent event;
    private final int stackOffset;
    private FastLocationInfo locationInfo;
    
    public FastLoggingEvent(LoggingEvent event, int stackOffset) {
        super(event.getFQNOfLoggerClass(),
              event.getLogger(),
              event.getLevel(),
              event.getMessage(),
              (event.getThrowableInformation() != null ? event.getThrowableInformation().getThrowable() : null));
        this.event = event;
        this.stackOffset = stackOffset;
    }
    
    public static int getStackOffset(LoggingEvent event) {
        Throwable t = new Throwable();
        String fqnOfCallingClass = event.getFQNOfLoggerClass();
        if (fqnOfCallingClass == null) return 0;

        // HACK
        StackTraceElement stack[] = t.getStackTrace();
        int offset = stack.length - 1;
        for (; offset >= 0; offset--) {
            if (stack[offset].getClassName().equals(fqnOfCallingClass)) break;
        } // FOR
        return (offset + 2);
    }
    
    @Override
    public LocationInfo getLocationInformation() {
        if (this.locationInfo == null) {
            StackTraceElement stack[] = Thread.currentThread().getStackTrace();
//            System.err.println(String.format("Stack=%d / Offset=%d", stack.length, this.stackOffset));
            if (this.stackOffset < stack.length) {
//                for (int i = 0; i < stack.length; i++) {
//                    System.err.printf("[%02d] %s\n", i, stack[i]);
//                }
                this.locationInfo = new FastLocationInfo(stack[this.stackOffset].getLineNumber(),
                                                         stack[this.stackOffset].getFileName(),
                                                         stack[this.stackOffset].getClassName(),
                                                         stack[this.stackOffset].getMethodName());
            }
        }
        return (this.locationInfo);
    }
    
    @Override
    public String getFQNOfLoggerClass() {
        return event.getFQNOfLoggerClass();
    }
    @Override
    public Level getLevel() {
        return event.getLevel();
    }
    @Override
    public Category getLogger() {
        return event.getLogger();
    }
    @Override
    public String getLoggerName() {
        return event.getLoggerName();
    }
    @Override
    public Object getMDC(String key) {
        return event.getMDC(key);
    }
    @Override
    public void getMDCCopy() {
        event.getMDCCopy();
    }
    @Override
    public Object getMessage() {
        return event.getMessage();
    }
    @Override
    public String getNDC() {
        return event.getNDC();
    }
    @SuppressWarnings("unchecked")
    @Override
    public Map getProperties() {
        return event.getProperties();
    }
    @SuppressWarnings("unchecked")
    @Override
    public Set getPropertyKeySet() {
        return event.getPropertyKeySet();
    }
    @Override
    public String getRenderedMessage() {
        return event.getRenderedMessage();
    }
    @Override
    public String getThreadName() {
        return event.getThreadName();
    }
    @Override
    public ThrowableInformation getThrowableInformation() {
        return event.getThrowableInformation();
    }
    @Override
    public String[] getThrowableStrRep() {
        return event.getThrowableStrRep();
    }
    
}
