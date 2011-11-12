package edu.brown.logging;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import junit.framework.TestCase;

public class TestFastLoggingEvent extends TestCase {

    class MockAppender extends AppenderSkeleton {
        @Override
        protected void append(LoggingEvent event) {
            TestFastLoggingEvent.this.offset = FastLoggingEvent.getStackOffset(event);
            TestFastLoggingEvent.this.origEvent = event;
            TestFastLoggingEvent.this.origEvent.getLocationInformation();
            TestFastLoggingEvent.this.fastEvent = new FastLoggingEvent(event, TestFastLoggingEvent.this.offset);
            TestFastLoggingEvent.this.fastEvent.getLocationInformation();
        }

        @Override
        public void close() {
            // Nothing
        }
        @Override
        public boolean requiresLayout() {
            return false;
        }
    }

    Logger logger = Logger.getRootLogger();
    int offset;
    LoggingEvent origEvent;
    FastLoggingEvent fastEvent;
    
    @Override
    protected void setUp() throws Exception {
        logger.addAppender(new MockAppender());
        logger.debug("TEST LOGGING EVENT");
        assertNotNull(this.origEvent);
        assertNotNull(this.fastEvent);
    }
    
    /**
     * testLocationInfo
     */
    public void testLocationInfo() throws Exception {
        LocationInfo expected = this.origEvent.getLocationInformation();
        assertNotNull(expected);
        
        LocationInfo actual = this.fastEvent.getLocationInformation();
        assertNotNull(actual);
        System.err.println(this.fastEvent.getLocationInformation());
        
        assertEquals(expected.getLineNumber(), actual.getLineNumber());
        assertEquals(expected.getFileName(), actual.getFileName());
        assertEquals(expected.getClassName(), actual.getClassName());
        assertEquals(expected.getMethodName(), actual.getMethodName());
    }
    
}
