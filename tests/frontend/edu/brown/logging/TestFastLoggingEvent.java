package edu.brown.logging;

import java.util.regex.Matcher;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import junit.framework.TestCase;

public class TestFastLoggingEvent extends TestCase {

    class MockAppender extends AppenderSkeleton {
        @Override
        protected void append(LoggingEvent event) {
//            System.err.println("Got event: " + event);
            
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

    Logger logger = Logger.getLogger(TestFastLoggingEvent.class);
    int offset;
    LoggingEvent origEvent;
    FastLoggingEvent fastEvent;
    
    @Override
    protected void setUp() throws Exception {
        logger.addAppender(new MockAppender());
        logger.info("TEST LOGGING EVENT");
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
    
    /**
     * testPreProcessorLocationInfoMatcher
     */
    public void testPreProcessorLocationInfoMatcher() {
        String codeLine = "ExecutionSite.java:1958"; 
        String msg = "Dispatching 1 messages and waiting for the results for InsertCallForwarding #1024611756678316032/0";
        Matcher m = FastLoggingEvent.PREPROCESSOR_PATTERN.matcher(codeLine + " " + msg);
        boolean find = m.find();
        System.err.println(m.group());
        assert(find);
        assertEquals(codeLine, m.group(1) + ":" + m.group(2));
        assertEquals(msg, m.replaceFirst(""));
    }
    
    /**
     * testPreProcessorLocationInfo
     */
    public void testPreProcessorLocationInfo() {
        String fileName = "ExecutionSite.java";
        String lineNum = "1958";
        String msg = "Dispatching 1 messages and waiting for the results for InsertCallForwarding #1024611756678316032/0";
        logger.info(String.format("%s:%s %s", fileName, lineNum, msg));
        assertNotNull(fastEvent);
        
        LocationInfo actual = this.fastEvent.getLocationInformation();
        assertNotNull(actual);
        assertEquals(fileName, actual.getFileName());
        assertEquals(lineNum, actual.getLineNumber());
        assertEquals(msg, fastEvent.getMessage());
    }
    
    
}
