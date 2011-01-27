package edu.brown.utils;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import junit.framework.TestCase;

public class TestLoggerUtil extends TestCase {

    private long rounds = 1000000000l;

    @Test
    public void testLoggerBoolean() {
        LoggerUtil.LoggerBoolean logger = new LoggerUtil.LoggerBoolean(true);
        for (long i = 0; i < rounds; i++) {
            logger.get();
        }
    }
    
    @Test
    public void testAtomicBoolean() {
        AtomicBoolean atomic = new AtomicBoolean(true);
        for (long i = 0; i < rounds; i++) {
            atomic.get();
        }
    }

    
}
