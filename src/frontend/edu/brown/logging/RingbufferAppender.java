package edu.brown.logging;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import org.apache.log4j.Layout;

/**
 * An appender that stores LoggingEvents in a ringbuffer (in-memory)
 * and allows to retrieve the latest log messages.
 * @author rschwarzkopf
 * http://ds.informatik.uni-marburg.de/~fallenbeck/ICSd/javadoc/0.9.16/de/fb12/ics/logging/RingbufferAppender.html
 */
public class RingbufferAppender extends AppenderSkeleton {
    private LoggingEvent[] rrStorage;
    private int storagePosition;
    private Layout layout;
    private long loggedLines;

    /**
     * Create an appender instance.
     * @param bufferSize The size of the ringbuffer.
     */
    public RingbufferAppender(int bufferSize, Layout layout) {
        this.rrStorage = new LoggingEvent[bufferSize];
        this.storagePosition = -1;
        this.layout = layout;
        this.loggedLines = 0;
    }
                 
    @Override
    protected void append(LoggingEvent event) {
        this.loggedLines++;
        this.rrStorage[this.storagePosition = ++this.storagePosition % this.rrStorage.length] = event;
    }

    @Override
    public void close() {
        // free memory
        for (int i = 0; i < this.rrStorage.length; i++)
            this.rrStorage[i] = null;
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }
    
    /**
     * Return the size of the ringbuffer.
     * @return Size of ringbuffer
     */
    public int getSize() {
        return this.rrStorage.length;
    }
    
    /**
     */
    /**
     */
    /**
     * Returns the number of lines logged since ICS startup
     * @return  Number of lines logged
     */
    public long getLoggedLines() {
        return this.loggedLines;
    }
    
//  public LoggingEvent[] getLogEntries() {
    public String[] getLogEntries() {
        // create a snapshot by copying the complete storage
        LoggingEvent[] events = new LoggingEvent[this.rrStorage.length];
        System.arraycopy(this.rrStorage, 0, events, 0, this.rrStorage.length);
        
        // store the index of the next element to be stored in the rrStorage
        // this should be the oldest entry
        int nextEntryIndex = (this.storagePosition + 1) % this.rrStorage.length;

        String[] sortedEvents;
        
        if (events[nextEntryIndex] == null) {
            // event entry has not yet been filled completely,
            // start at zero
            sortedEvents = new String[nextEntryIndex];
            for (int i = 0; i < sortedEvents.length; i++) {
                sortedEvents[i] = this.layout.format(events[i]);
            }
        } else {
            // the storage position might have changed between copying the array and
            // calculating the next entry index, compare with oldest entry
            
            // find the oldest entry by comparing the timestamps
            int oldestEntryIndex = -1;
            long oldestEntryTimestamp = Long.MAX_VALUE;
            LoggingEvent entry;
            long entryTimestamp;
            for (int entryIndex = 0; entryIndex < events.length; entryIndex++)
                // FIXME <= ? start at the nextEntryIndex? Maybe backwards?
                if ((entry = events[entryIndex]) != null && (entryTimestamp = entry.getTimeStamp()) < oldestEntryTimestamp) {
                    oldestEntryIndex = entryIndex;
                    oldestEntryTimestamp = entryTimestamp;
                }
            
            // if the next entry index and oldest entry index are different and have the same timestamp
            // (which only happens if much logging is done), use the next entry index
            
            if (nextEntryIndex != oldestEntryIndex && oldestEntryTimestamp == events[nextEntryIndex].getTimeStamp())
                oldestEntryIndex = nextEntryIndex;
            
            // sort the events into a new array
            //  LoggingEvent[] sortedEvents = new LoggingEvent[events.length];
            sortedEvents = new String[events.length];
            for (int i = 0; i < sortedEvents.length; i++) {
                sortedEvents[i] = this.layout.format(events[oldestEntryIndex++ % events.length]);
            }
        }
        
        // throw away the old array
        events = null;
        
        return sortedEvents;
    }
}
