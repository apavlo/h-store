package edu.brown.logging;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

public class FastLoggingEvent extends LoggingEvent {
    private static final Logger LOG = Logger.getLogger(FastLoggingEvent.class);
    private static final long serialVersionUID = -5595734504055550617L;
    
    protected static final Pattern PREPROCESSOR_PATTERN = Pattern.compile("^([\\w]+\\.java):([\\d]+) "); 
    
    private final LoggingEvent event;
    private final int stackOffset;
    private FastLocationInfo locationInfo;
    private String cleanMessage = null;
    
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
            // HACK: Use preprocessor information
            String msg = this.event.getMessage().toString();
            Matcher m = PREPROCESSOR_PATTERN.matcher(msg);
            if (LOG.isDebugEnabled()) LOG.debug("Checking whether we can use PREPROCESSOR info for location: " + msg);
            
            if (m.find()) {
                if (LOG.isDebugEnabled()) LOG.debug("Using preprocessor information get source location [" + m + "]");
                
                String fileName = m.group(1);
                int lineNumber = Integer.parseInt(m.group(2));
                this.locationInfo = new FastLocationInfo(lineNumber, fileName, "", "");
                this.cleanMessage = m.replaceFirst("");
            } else {
                if (LOG.isDebugEnabled()) LOG.debug("Using stack offset lookup to get source location");
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
        return (this.cleanMessage != null ? this.cleanMessage : this.event.getMessage());
    }
    @Override
    public String getRenderedMessage() {
        return (this.cleanMessage != null ? this.cleanMessage : this.event.getRenderedMessage());
    }
    @Override
    public String getNDC() {
        return event.getNDC();
    }
    @SuppressWarnings("rawtypes")
    @Override
    public Map getProperties() {
        return event.getProperties();
    }
    @SuppressWarnings("rawtypes")
    @Override
    public Set getPropertyKeySet() {
        return event.getPropertyKeySet();
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
