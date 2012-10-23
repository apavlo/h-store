package edu.brown.profilers;

public class NetworkProfiler extends AbstractProfiler {

    /**
     * 
     */
    public final ProfileMeasurement network_processing = new ProfileMeasurement("PROCESSING");
    
    /**
     * How much time the site spends with backup pressure blocking disabled
     */
    public final ProfileMeasurement network_backup_off = new ProfileMeasurement("BACKUP_OFF");
    
    /**
     * How much time the site spends with backup pressure blocking enabled
     */
    public final ProfileMeasurement network_backup_on = new ProfileMeasurement("BACKUP_ON");
    
}
