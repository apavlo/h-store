package edu.brown.logging;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * An appender that stores LoggingEvents in a ringbuffer (in-memory)
 * and allows to retrieve the latest log messages.
 * @author rschwarzkopf
 * @author pavlo
 * http://ds.informatik.uni-marburg.de/~fallenbeck/ICSd/javadoc/0.9.16/de/fb12/ics/logging/RingbufferAppender.html
 */
public class RingBufferAppender extends AppenderSkeleton {
    private static final Logger LOG = Logger.getLogger(RingBufferAppender.class);
    
    private static final int DEFAULT_SIZE = 1000;
    
    private LoggingEvent[] eventRing;
    private int currentPosition;
    private long counter;
    private int stackOffset = -1;
    
    private boolean useFastLocation = false;
    private boolean storeLocation = false;
    private boolean storeThreadName = false;
    
    /**
     * Create an appender instance.
     * @param bufferSize The size of the ringbuffer.
     */
    public RingBufferAppender() {
        this.init(DEFAULT_SIZE);
    }
    
    public RingBufferAppender(int size) {
        this.init(size);
    }
    
    private void init(int size) {
        this.eventRing = new LoggingEvent[size];
        this.currentPosition = -1;
        this.counter = 0;
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Initialized appender with new buffer [size=%d, useFastLocation=%s, storeLocation=%s, storeThreadName=%s, layout=%s]",
                                    size, this.useFastLocation, this.storeLocation, this.storeThreadName,
                                    (this.getLayout() != null ? this.getLayout().getClass().getSimpleName() : null)));
    }
    
    public void setUseFastLocation(boolean val) {
        this.useFastLocation = val;
    }
    public void setStoreLocation(boolean val) {
        this.storeLocation = val;
    }
    public void setStoreThreadName(boolean val) {
        this.storeThreadName = val;
    }
    
    public void setSize(int size) {
        this.init(size);
    }
    /**
     * Return the size of the ringbuffer.
     * @return Size of ringbuffer
     */
    public int getSize() {
        return this.eventRing.length;
    }
                 
    @Override
    protected void append(LoggingEvent event) {
        if (this.useFastLocation) {
            if (this.stackOffset < 0) this.stackOffset = FastLoggingEvent.getStackOffset(event);
            event = new FastLoggingEvent(event, this.stackOffset); 
        }
        if (this.storeLocation) event.getLocationInformation();
        if (this.storeThreadName) event.getThreadName();
        
        int position = -1;
        synchronized (this) {
            this.currentPosition = position = ++this.currentPosition % this.eventRing.length;
            this.counter++;
        } // SYNCH
        this.eventRing[position] = event;
//        assert(event.getLoggerName().contains("Handler") == false) : event;
    }

    @Override
    public void close() {
        // free memory
        for (int i = 0; i < this.eventRing.length; i++)
            this.eventRing[i] = null;
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }
    
    /**
     * Returns the number of lines logged since ICS startup
     * @return  Number of lines logged
     */
    public long getLoggedLines() {
        return this.counter;
    }
    
    public LoggingEvent[] getLogEvents() {
        // create a snapshot by copying the complete storage
        LoggingEvent[] events = new LoggingEvent[this.eventRing.length];
        System.arraycopy(this.eventRing, 0, events, 0, this.eventRing.length);
        
        // store the index of the next element to be stored in the rrStorage
        // this should be the oldest entry
        int nextEntryIndex = (this.currentPosition + 1) % this.eventRing.length;
        
        LoggingEvent[] sortedEvents;
        if (events[nextEntryIndex] == null) {
            // event entry has not yet been filled completely, start at zero
            sortedEvents = new LoggingEvent[nextEntryIndex];
            for (int i = 0; i < sortedEvents.length; i++) {
                sortedEvents[i] = events[i];
            }
            
        // the storage position might have changed between copying the array and
        // calculating the next entry index, compare with oldest entry
        } else {
            // find the oldest entry by comparing the timestamps
            int oldestEntryIndex = -1;
            long oldestEntryTimestamp = Long.MAX_VALUE;
            LoggingEvent entry;
            long entryTimestamp;
            for (int entryIndex = 0; entryIndex < events.length; entryIndex++) {
                // FIXME <= ? start at the nextEntryIndex? Maybe backwards?
                if ((entry = events[entryIndex]) != null && (entryTimestamp = entry.getTimeStamp()) < oldestEntryTimestamp) {
                    oldestEntryIndex = entryIndex;
                    oldestEntryTimestamp = entryTimestamp;
                }
            } // FOR
            
            // if the next entry index and oldest entry index are different and have the same timestamp
            // (which only happens if much logging is done), use the next entry index
            if (nextEntryIndex != oldestEntryIndex && oldestEntryTimestamp == events[nextEntryIndex].getTimeStamp())
                oldestEntryIndex = nextEntryIndex;
            
            // sort the events into a new array
            //  LoggingEvent[] sortedEvents = new LoggingEvent[events.length];
            sortedEvents = new LoggingEvent[events.length];
            for (int i = 0; i < sortedEvents.length; i++) {
                sortedEvents[i] = events[oldestEntryIndex++ % events.length];
            }
        }
        // throw away the old array
        events = null;
        
        return (sortedEvents);
    }

    public String[] getLogMessages() {
        LoggingEvent[] events = this.getLogEvents();
        String[] ret = new String[events.length];
        Layout layout = this.getLayout();
        for (int i = 0; i < events.length; i++) {
            ret[i] = layout.format(events[i]);
        } // FOR
        return (ret);
    }
    
    
    @SuppressWarnings("unchecked")
    public static void enableRingBufferAppender(Logger logger, int bufferSize) {
        Layout l = null;
        if (LOG.isDebugEnabled())
            LOG.debug(logger + " => " + logger.getAllAppenders());
        for (Object o : CollectionUtil.iterable(logger.getAllAppenders())) {
            Appender a = (Appender)o;
            l = a.getLayout();
        } // FOR
        if (l != null) {
            logger.removeAllAppenders();
            logger.addAppender(new RingBufferAppender(bufferSize));
            Logger.getRootLogger().info("Enabled RingBuffer logging for '" + logger.getName() + "'");
        }        
    }
    
    @SuppressWarnings("unchecked")
    public static RingBufferAppender getRingBufferAppender(Logger logger) {
        RingBufferAppender rba = null;
        if (LOG.isTraceEnabled())
            LOG.trace("Checking whether " + logger.getName() + " has a RingBufferAppender attached: " + CollectionUtil.list(logger.getAllAppenders()));
        for (Object o : CollectionUtil.iterable(logger.getAllAppenders())) {
            if (o instanceof RingBufferAppender) {
                rba = (RingBufferAppender)o;
                if (LOG.isDebugEnabled())
                    LOG.debug("Found " + rba + " for " + logger.getName());
                break;
            }
        } // FOR
        return (rba);
    }
    
    @SuppressWarnings("unchecked")
    public static Collection<LoggingEvent> getLoggingEvents(LoggerRepository repo) {
        Set<Logger> loggers = new HashSet<Logger>();
        for (Object o : CollectionUtil.iterable(repo.getCurrentLoggers())) {
            Logger logger = (Logger)o;
            RingBufferAppender rba = getRingBufferAppender(logger);
            if (rba != null) {
                if (LOG.isDebugEnabled())
                    LOG.debug(logger.getName() + " => " + rba + " / " + rba.getLayout());
                loggers.add(logger);
            }
        } // FOR
        if (loggers.isEmpty()) return (Collections.emptyList());
        return (getLoggingEvents(loggers.toArray(new Logger[0]))); 
    }
    
    @SuppressWarnings("unchecked")
    public static Collection<String> getLoggingMessages(LoggerRepository repo) {
        Set<RingBufferAppender> appenders = new HashSet<RingBufferAppender>();
        for (Object o : CollectionUtil.iterable(repo.getCurrentLoggers())) {
            Logger logger = (Logger)o;
            RingBufferAppender rba = getRingBufferAppender(logger);
            if (rba != null) {
                 if (LOG.isDebugEnabled())
                    LOG.debug(logger.getName() + " => " + rba + " / " + rba.getLayout());
                appenders.add(rba);
            }
        } // FOR
        if (appenders.isEmpty()) return (Collections.emptyList());
        return (getLoggingMessages(appenders.toArray(new RingBufferAppender[0]))); 
    }
    
    public static Collection<LoggingEvent> getLoggingEvents(Logger...loggers) {
        SortedSet<LoggingEvent> events = new TreeSet<LoggingEvent>(new Comparator<LoggingEvent>() {
            @Override
            public int compare(LoggingEvent o1, LoggingEvent o2) {
                return (int)(o1.timeStamp - o2.timeStamp);
            }
        });
        for (Logger log : loggers) {
            RingBufferAppender rba = getRingBufferAppender(log);
            if (rba != null) {
                CollectionUtil.addAll(events, rba.getLogEvents());
            }
        } // FOR
        return (events);
    }
    
    public static Collection<String> getLoggingMessages(RingBufferAppender...appenders) {
        List<LoggingEvent> events = new ArrayList<LoggingEvent>();
        Layout layout = null; 
        for (RingBufferAppender rba : appenders) {
            LoggingEvent e[] = rba.getLogEvents();
            if (LOG.isDebugEnabled())
                LOG.debug("Got " + e.length + " LoggingEvents for " + rba);
            CollectionUtil.addAll(events, e);
            if (layout == null) layout = rba.getLayout();
        } // FOR
        if (events.isEmpty() == false) assert(layout != null);
        
        Collections.sort(events, new Comparator<LoggingEvent>() {
            @Override
            public int compare(LoggingEvent o1, LoggingEvent o2) {
                return (int)(o1.timeStamp - o2.timeStamp);
            }
        });
        List<String> messages = new ArrayList<String>();
        for (LoggingEvent event : events) {
            messages.add(layout.format(event));
        } // FOR
        return (messages);
    }
    
    public void dump(PrintStream out) {
        int width = 100;
        out.println(StringUtil.header(this.getClass().getSimpleName(), "=", width));
        for (String log : this.getLogMessages()) {
            out.println(log.trim());
        }
        out.println(StringUtil.repeat("=", width));
        out.flush();
    }
}
