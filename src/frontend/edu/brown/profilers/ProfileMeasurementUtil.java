package edu.brown.profilers;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;

public abstract class ProfileMeasurementUtil {
    private static final Logger LOG = Logger.getLogger(ProfileMeasurementUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // --------------------------------------------------------------------------------------------
    // SWAP METHODS
    // --------------------------------------------------------------------------------------------
    
    /**
     * Stop one of the given ProfileMeasurement handles and start the other
     * 
     * @param to_stop
     *            the handle to stop
     * @param to_start
     *            the handle to start
     */
    public static void swap(ProfileMeasurement to_stop, ProfileMeasurement to_start) {
        ProfileMeasurementUtil.swap(ProfileMeasurement.getTime(), to_stop, to_start);
    }

    public static void swap(long timestamp, ProfileMeasurement to_stop, ProfileMeasurement to_start) {
        if (debug.val)
            LOG.debug(String.format("SWAP %s -> %s", to_stop, to_start));
        to_stop.stopIfStarted(timestamp);
        to_start.start(timestamp);
    }

    // --------------------------------------------------------------------------------------------
    // STOP METHODS
    // --------------------------------------------------------------------------------------------
    
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
    
    /**
     * Stop multiple ProfileMeasurements with the same timestamp If
     * ignore_stopped is true, we won't stop ProfileMeasurements that already
     * stopped
     * 
     * @param ignore_stopped
     * @param to_stop
     */
    public static void stop(boolean ignore_stopped, ProfileMeasurement... to_stop) {
        long time = ProfileMeasurement.getTime();
        for (ProfileMeasurement pm : to_stop) {
            synchronized (pm) {
                if (ignore_stopped == false || (ignore_stopped && pm.isStarted()))
                    pm.stop(time);
            } // SYNCH
        } // FOR
    }

    /**
     * Stop multiple ProfileMeasurements with the same timestamp
     * 
     * @param to_stop
     */
    public static void stop(ProfileMeasurement... to_stop) {
        stop(false, to_stop);
    }
    
    // --------------------------------------------------------------------------------------------
    // START METHODS
    // --------------------------------------------------------------------------------------------

    public static void start(boolean ignore_started, ProfileMeasurement... to_start) {
        long time = ProfileMeasurement.getTime();
        for (ProfileMeasurement pm : to_start) {
            synchronized (pm) {
                if (ignore_started == false || (ignore_started && pm.isStarted() == false))
                    pm.start(time);
            } // SYNCH
        } // FOR
    }

    /**
     * Start multiple ProfileMeasurements with the same timestamp
     * 
     * @param to_start
     */
    public static void start(ProfileMeasurement... to_start) {
        start(false, to_start);
    }
    
    // --------------------------------------------------------------------------------------------
    // PRETTY PRINTING
    // --------------------------------------------------------------------------------------------

    public static String debug(ProfileMeasurement pm) {
        return String.format("%.2fms total / %s avg",
                             pm.getTotalThinkTimeMS(),
                             StringUtil.formatTime("%.2f", pm.getAverageThinkTime()));
    }
    
    /**
     * Create a single-line that shows the delta for a given ProfileMeasurement
     * from a previous one.
     * @param pm
     * @param last
     * @param showInvocations
     * @return
     */
    public static String formatComparison(ProfileMeasurement pm, ProfileMeasurement last,
                                          boolean showInvocations) {
        final String separator = "/";
        StringBuilder value = new StringBuilder();
        if (showInvocations) {
            value.append(String.format("%d invocations %s ", pm.getInvocations(), separator)); 
        }
        
        value.append(ProfileMeasurementUtil.debug(pm));
        if (last != null) {
            value.append(" [");
            for (int i = 0; i < 2; i++) {
                if (i > 0) value.append(" ").append(separator).append(" ");
                String deltaPrefix = "";
                double delta;
                // TOTAL
                if (i == 0) {
                    delta = pm.getTotalThinkTime() - last.getTotalThinkTime();
                    deltaPrefix = "TOTAL: ";
                }
                // AVERAGE
                else {
                    delta = pm.getAverageThinkTime() - last.getAverageThinkTime();
                    deltaPrefix = "AVG: ";
                }
                String deltaTime = StringUtil.formatTime("%.2f", delta, TimeUnit.NANOSECONDS);
                String deltaArrow = "";
                if (delta > 0) {
                    deltaArrow = StringUtil.UNICODE_UP_ARROW;
                } else if (delta < 0) {
                    deltaArrow = StringUtil.UNICODE_DOWN_ARROW;
                }
                value.append(deltaPrefix + deltaArrow + deltaTime);
            } // FOR
            value.append("]");
        }
        return (value.toString());
    }
    
}
