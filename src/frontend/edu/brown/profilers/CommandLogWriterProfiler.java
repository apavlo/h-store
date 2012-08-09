package edu.brown.profilers;

public class CommandLogWriterProfiler extends AbstractProfiler {

    public final ProfileMeasurement writingTime = new ProfileMeasurement("WRITING");
    public final ProfileMeasurement blockedTime = new ProfileMeasurement("BLOCKED");
    public final ProfileMeasurement networkTime = new ProfileMeasurement("NETWORK");
    
}
