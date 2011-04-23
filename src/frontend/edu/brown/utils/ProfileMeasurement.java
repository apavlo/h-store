package edu.brown.utils;

import org.apache.log4j.Logger;

/**
 * 
 * @author pavlo
 */
public class ProfileMeasurement {
    public static final Logger LOG = Logger.getLogger(ProfileMeasurement.class);
    
    public enum Type {
        /** The total amount of time spent for a transactions */
        TOTAL,
        /** Initialization time **/
        INITIALIZATION,
        /** Blocked time **/
        BLOCKED,
        /** Clean-up time **/
        CLEANUP,
        /** The time spent waiting in the execution queue **/
        QUEUE,
        /** The amount of time spent executing the Java-portion of the stored procedure */
        JAVA,
        /** The amount of time spent coordinating the transaction */
        COORDINATOR,
        /** The amount of time spent generating the execution plans for the transaction */
        PLANNER,
        /** The amount of time spent executing in the plan fragments */
        EE,
        /** The amount of time spent estimating what the transaction will do */
        ESTIMATION,
        /** Anything else... */
        MISC;
    }

    /**
     * The profile type
     */
    private final Type type;
    /**
     * Total amount of time spent processsing the profiled seciton (in ms) 
     */
    private transient long think_time;
    /**
     * This marker is used to set when the boundary area of the code
     * we are trying to profile starts and stops. When it is zero, the
     * system is outside of the profiled area. 
     */
    private transient Long think_marker; 

    /**
     * Constructor
     * @param pmtype
     */
    public ProfileMeasurement(Type pmtype) {
        this.type = pmtype;
        this.reset();
    }

    public void reset() {
        this.think_marker = null;
        this.think_time = 0;
//        if (type == Type.JAVA) LOG.info(String.format("RESET %s [%d]", this.type, this.hashCode()));
    }

    /**
     * Get the profile type
     * @return
     */
    public Type getType() {
        return type;
    }

    /**
     * Get the total amount of time spent in the profiled area
     * @return
     */
    public long getTotalThinkTime() {
        return (this.think_time);
    }
    
    // ----------------------------------------------------------------------------
    // START METHODS
    // ----------------------------------------------------------------------------

    /**
     * Main method for stop this ProfileMeasurement from recording time
     * @return this
     */

    public ProfileMeasurement start(long time) {
        assert(this.think_marker == null) : this.type + " - " + this.hashCode();
        this.think_marker = time;
//        if (type == Type.JAVA) LOG.info(String.format("START %s [%d]", this.type, this.hashCode()));
        return (this);
    }
    
    public ProfileMeasurement start() {
        return (this.start(getTime()));
    }
    
    public boolean isStarted() {
        return (this.think_marker != null);
    }

    // ----------------------------------------------------------------------------
    // STOP METHODS
    // ----------------------------------------------------------------------------

    /**
     * Main method for stop this ProfileMeasurement from recording time
     * We will check to make sure that this handle was started first
     * @return this
     */
    public ProfileMeasurement stop(long time) {
        assert(this.think_marker != null) : this.type + " - " + this.hashCode();
        long added = (time - this.think_marker);
        this.think_time += added;
        this.think_marker = null;
//        if (type == Type.JAVA) LOG.info(String.format("STOP %s [time=%d, id=%d]", this.type, added, this.hashCode()));
        return (this);
    }

    public ProfileMeasurement stop() {
        return (this.stop(getTime()));
    }

    public boolean isStopped() {
        return (this.think_marker == null);
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public void appendTime(ProfileMeasurement other) {
        assert(other != null);
        assert(this.type == other.type);
        this.think_time += other.think_time;
        this.think_marker = other.think_marker;
    }
 
    public void addThinkTime(long start, long stop) {
        assert(this.think_marker == null) : this.type;
        this.think_time += (stop - start);
    }

    /**
     * Return the current time in nano-seconds
     * @return
     */
    public static long getTime() {
//      return System.currentTimeMillis();
      return System.nanoTime();
    }
    
    /**
     * Start multiple ProfileMeasurements with the same timestamp
     * @param to_start
     */
    public static void start(ProfileMeasurement...to_start) {
        long time = ProfileMeasurement.getTime();
        for (ProfileMeasurement pm : to_start) {
            pm.start(time);
        } // FOR
    }
    
    /**
     * Stop multiple ProfileMeasurements with the same timestamp
     * @param to_stop
     */
    public static void stop(ProfileMeasurement...to_stop) {
        long time = ProfileMeasurement.getTime();
        for (ProfileMeasurement pm : to_stop) {
            pm.stop(time);
        } // FOR
    }
    
    /**
     * Stop one of the given ProfileMeasurement handles and start the other
     * @param to_stop the handle to stop
     * @param to_start the handle to start
     */
    public static void swap(ProfileMeasurement to_stop, ProfileMeasurement to_start) {
        long time = ProfileMeasurement.getTime();
        to_stop.stop(time);
        to_start.start(time);
    }

    @Override
    public String toString() {
        return (String.format("%s[total=%d, marker=%s]", this.type, this.think_time, this.think_marker));
    }
}