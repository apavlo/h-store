/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.utils;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * @author pavlo
 */
public class ProfileMeasurement implements JSONSerializable {
    public static final Logger LOG = Logger.getLogger(ProfileMeasurement.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /** The profile type */
    private String type;
    /** Total amount of time spent processing the profiled section (in ms) */
    private long total_time;
    /** The number of times that this ProfileMeasurement has been started */
    private int invocations = 0;

    /**
     * This marker is used to set when the boundary area of the code we are
     * trying to profile starts and stops. When it is zero, the system is
     * outside of the profiled area.
     */
    private transient Long think_marker;

    private transient boolean reset = false;

    private transient EventObservable<ProfileMeasurement> start_observable;
    private transient EventObservable<ProfileMeasurement> stop_observable;

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    public ProfileMeasurement() {
        // For serialization
    }

    /**
     * Constructor
     * 
     * @param pmtype
     */
    public ProfileMeasurement(String pmtype) {
        this.type = pmtype;
        this.reset();
    }

    /**
     * Copy constructor
     * 
     * @param orig
     */
    public ProfileMeasurement(ProfileMeasurement orig) {
        this(orig.type);
        this.appendTime(orig);
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    public synchronized void reset() {
        if (this.think_marker != null) {
            this.reset = true;
        }
        this.total_time = 0;
        this.invocations = 0;
    }

    public void clear() {
        this.think_marker = null;
        this.invocations = 0;
        this.total_time = 0;
    }

    /**
     * Reset this ProfileMeasurements internal data when an update arrives
     * for the given EventObservable
     * @param e
     */
    public <T> void resetOnEventObservable(EventObservable<T> e) {
        e.addObserver(new EventObserver<T>() {
            @Override
            public void update(EventObservable<T> o, T arg) {
                ProfileMeasurement.this.reset();
            }
        });
    }

    /**
     * Get the profile type
     * 
     * @return
     */
    public String getType() {
        return type;
    }

    /**
     * Get the total amount of time spent in the profiled area in nanoseconds
     * 
     * @return
     */
    public long getTotalThinkTime() {
        return (this.total_time);
    }

    /**
     * Get the total amount of time spent in the profiled area in milliseconds
     * 
     * @return
     */
    public double getTotalThinkTimeMS() {
        return (this.total_time / 1000000d);
    }

    /**
     * Get the total amount of time spent in the profiled area in seconds
     * 
     * @return
     */
    public double getTotalThinkTimeSeconds() {
        return (this.total_time / 1000000d / 1000d);
    }

    /**
     * Get the average think time per invocation in nanoseconds
     * 
     * @return
     */
    public double getAverageThinkTime() {
        return (this.invocations > 0 ? this.total_time / (double) this.invocations : 0d);
    }

    /**
     * Get the average think time per invocation in milliseconds
     * 
     * @return
     */
    public double getAverageThinkTimeMS() {
        return (this.getAverageThinkTime() / 1000000d);
    }

    /**
     * Get the total number of times this object was started
     * 
     * @return
     */
    public int getInvocations() {
        return (this.invocations);
    }

    // ----------------------------------------------------------------------------
    // START METHODS
    // ----------------------------------------------------------------------------

    /**
     * Main method for stop this ProfileMeasurement from recording time
     * 
     * @return this
     */

    public synchronized ProfileMeasurement start(long timestamp) {
        assert (this.think_marker == null) : String.format("Trying to start %s before it was stopped!", this.type);
        if (debug.get())
            LOG.debug(String.format("START %s", this));
        this.think_marker = timestamp;
        this.invocations++;
        if (this.start_observable != null)
            this.start_observable.notifyObservers(this);
        return (this);
    }

    public ProfileMeasurement start() {
        return (this.start(getTime()));
    }

    public boolean isStarted() {
        return (this.think_marker != null);
    }

    public synchronized void addStartObserver(EventObserver<ProfileMeasurement> observer) {
        if (this.start_observable == null) {
            this.start_observable = new EventObservable<ProfileMeasurement>();
        }
        this.start_observable.addObserver(observer);
    }

    // ----------------------------------------------------------------------------
    // STOP METHODS
    // ----------------------------------------------------------------------------

    /**
     * Main method for stop this ProfileMeasurement from recording time We will
     * check to make sure that this handle was started first
     * 
     * @return this
     */
    public synchronized ProfileMeasurement stop(long timestamp) {
        if (this.reset) {
            this.reset = false;
            this.think_marker = null;
            return (this);
        }
        if (debug.get())
            LOG.debug(String.format("STOP %s", this));
        assert (this.think_marker != null) : String.format("Trying to stop %s before it was started!", this.type);
        long added = (timestamp - this.think_marker);
        if (added < 0) {
            LOG.warn(String.format("Invalid stop timestamp for %s [timestamp=%d, marker=%d, added=%d]", this.type, timestamp, this.think_marker, added));
        } else {
            this.total_time += added;
        }
        this.think_marker = null;
        if (this.stop_observable != null)
            this.stop_observable.notifyObservers(this);
        // if (type == Type.JAVA)
        // LOG.info(String.format("STOP %s [time=%d, id=%d]", this.type, added,
        // this.hashCode()));
        return (this);
    }

    public ProfileMeasurement stop() {
        return (this.stop(getTime()));
    }

    public boolean isStopped() {
        return (this.think_marker == null);
    }

    public synchronized void addStopObserver(EventObserver<ProfileMeasurement> observer) {
        if (this.stop_observable == null) {
            this.stop_observable = new EventObservable<ProfileMeasurement>();
        }
        this.stop_observable.addObserver(observer);
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    public ProfileMeasurement appendTime(ProfileMeasurement other, boolean checkType) {
        assert (other != null);
        if (checkType)
            assert (this.type == other.type);
        this.total_time += other.total_time;
        this.think_marker = other.think_marker;
        this.invocations += other.invocations;
        return (this);
    }

    public ProfileMeasurement appendTime(ProfileMeasurement other) {
        return (this.appendTime(other, false));
    }

    public void addThinkTime(long start, long stop, int invocations) {
        assert (this.think_marker == null) : this.type;
        this.total_time += (stop - start);
        this.invocations += invocations;
    }

    public void addThinkTime(long start, long stop) {
        this.addThinkTime(start, stop, 0);
    }

    /**
     * Return the current time in nano-seconds
     * 
     * @return
     */
    public static long getTime() {
        // return System.currentTimeMillis();
        return System.nanoTime();
    }

    /**
     * Start multiple ProfileMeasurements with the same timestamp
     * 
     * @param to_start
     */
    public static void start(ProfileMeasurement... to_start) {
        start(false, to_start);
    }

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
     * Stop multiple ProfileMeasurements with the same timestamp
     * 
     * @param to_stop
     */
    public static void stop(ProfileMeasurement... to_stop) {
        stop(false, to_stop);
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
                if (ignore_stopped == false || (ignore_stopped && pm.isStopped() == false))
                    pm.stop(time);
            } // SYNCH
        } // FOR
    }

    /**
     * Stop one of the given ProfileMeasurement handles and start the other
     * 
     * @param to_stop
     *            the handle to stop
     * @param to_start
     *            the handle to start
     */
    public static void swap(ProfileMeasurement to_stop, ProfileMeasurement to_start) {
        swap(ProfileMeasurement.getTime(), to_stop, to_start);
    }

    public static void swap(long timestamp, ProfileMeasurement to_stop, ProfileMeasurement to_start) {
        if (debug.get())
            LOG.debug(String.format("SWAP %s -> %s", to_stop, to_start));
        to_stop.stop(timestamp);
        to_start.start(timestamp);
    }

    @Override
    public String toString() {
        return (this.debug(false));
    }

    public String debug() {
        return this.debug(true);
    }

    public String debug(boolean verbose) {
        if (verbose) {
            return (String.format("%s[total=%d, marker=%s, invocations=%d, avg=%.2f ms]", this.type, this.total_time, this.think_marker, this.invocations, this.getAverageThinkTimeMS()));
        } else {
            return (this.type);
        }
    }

    // --------------------------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // --------------------------------------------------------------------------------------------

    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        stringer.key("TYPE").value(this.type);
        stringer.key("TIME").value(this.total_time);
        stringer.key("INVOCATIONS").value(this.invocations);
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        this.type = json_object.getString("TYPE");
        this.total_time = json_object.getLong("TIME");
        this.invocations = json_object.getInt("INVOCATIONS");
    }
}