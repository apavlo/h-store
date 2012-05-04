package org.voltdb.client;

public interface ClientStatsLoader {
    public void start(long startTime, int leaderAddress) throws Exception;
    public void stop() throws InterruptedException;
}
