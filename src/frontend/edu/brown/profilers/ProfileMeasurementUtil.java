package edu.brown.profilers;

public abstract class ProfileMeasurementUtil {

    /**
     * Stop the given ProfileMeasurements at the given timestamp if they 
     * have been started.
     * @param timestamp
     * @param pms
     */
    public static void stopIfStarted(long timestamp, ProfileMeasurement...pms) {
        for (ProfileMeasurement pm : pms) {
            if (pm.isStarted()) pm.stop(timestamp);
        } // FOR
    }
    
}
