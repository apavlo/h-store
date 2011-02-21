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
        /** The amount of time spent executing the Java-portion of the stored procedure */
        JAVA,
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
    
    public synchronized void startThinkMarker(long time) {
        assert(this.think_marker == null) : this.type + " - " + this.hashCode();
        this.think_marker = time;
//        LOG.info(String.format("START %s [%d]", this.type, this.hashCode()));
    }
    
    public void startThinkMarker() {
        this.startThinkMarker(System.currentTimeMillis());
    }
    
    public boolean isStarted() {
        return (this.think_marker != null);
    }
    
    public synchronized void stopThinkMarker(long time) {
        assert(this.think_marker != null) : this.type + " - " + this.hashCode();
        this.think_time += (time - this.think_marker);
        this.think_marker = null;
//        LOG.info(String.format("STOP %s [%d]", this.type, this.hashCode()));
    }
    
    public boolean isStopped() {
        return (this.think_marker == null);
    }
    
    public void stopThinkMarker() {
        this.stopThinkMarker(System.currentTimeMillis());
    }
    
    public void reset() {
        this.think_marker = null;
        this.think_time = 0;
//        LOG.info(String.format("RESET %s [%d]", this.type, this.hashCode()));
    }
    
}