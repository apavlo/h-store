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
package edu.brown.profilers;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.collections15.Buffer;
import org.apache.commons.collections15.buffer.CircularFifoBuffer;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

/**
 * @author pavlo
 */
public class ProfileMeasurement implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(ProfileMeasurement.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final long NULL_MARKER = -1l;
    
    /** The profile type */
    private String name;
    /** Total amount of time spent processing the profiled section (in ms) */
    private long total_time;
    /** The number of times that this ProfileMeasurement has been started */
    private int invocations = 0;

    private Buffer<Long> history = null;
    
    /**
     * This marker is used to set when the boundary area of the code we are
     * trying to profile starts and stops. When it is zero, the system is
     * outside of the profiled area.
     */
    private transient long marker = NULL_MARKER;

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
     * @param pmtype
     */
    public ProfileMeasurement(String pmtype) {
        this(pmtype, false);
    }
    
    /**
     * Constructor
     * @param pmtype
     * @param history Enable history tracking
     */
    public ProfileMeasurement(String pmtype, boolean history) {
        this.name = pmtype;
        if (history) this.enableHistoryTracking();
        this.reset();
    }

    /**
     * Copy constructor
     * 
     * @param orig
     */
    public ProfileMeasurement(ProfileMeasurement orig) {
        this(orig.name, (orig.history != null));
        if (this.history != null) {
            this.history.addAll(orig.history);
        }
        this.appendTime(orig);
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    public void enableHistoryTracking() {
        if (this.history == null) {
            synchronized (this) {
                if (this.history == null) {
                    this.history = new CircularFifoBuffer<Long>(10000);
                }
            } // SYNCH
            if (debug.val)
                LOG.debug("Enabled history tracking in " + this);
        }
    }
    
    public void reset() {
        if (this.marker != NULL_MARKER) {
            this.reset = true;
        }
        this.total_time = 0;
        this.invocations = 0;
        if (this.history != null) this.history.clear();
    }

    public void clear() {
        this.marker = NULL_MARKER;
        this.invocations = 0;
        this.total_time = 0;
        if (this.history != null) this.history.clear();
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
     * Get the profile type name
     * @return
     */
    public String getName() {
        return (this.name);
    }

    /**
     * Get the total amount of time spent in the profiled area in nanoseconds
     * @return
     */
    public long getTotalThinkTime() {
        return (this.total_time);
    }

    /**
     * Get the total amount of time spent in the profiled area in milliseconds
     * @return
     */
    public double getTotalThinkTimeMS() {
        return (this.total_time / 1000000d);
    }

    /**
     * Get the total amount of time spent in the profiled area in seconds
     * @return
     */
    public double getTotalThinkTimeSeconds() {
        return (this.total_time / 1000000d / 1000d);
    }

    /**
     * Get the average think time per invocation in nanoseconds
     * @return
     */
    public double getAverageThinkTime() {
        return (this.invocations > 0 ? this.total_time / (double) this.invocations : 0d);
    }

    /**
     * Get the average think time per invocation in milliseconds
     * @return
     */
    public double getAverageThinkTimeMS() {
        return (this.getAverageThinkTime() / 1000000d);
    }

    /**
     * Get the total number of times this object was started
     * @return
     */
    public int getInvocations() {
        return (this.invocations);
    }

    protected long getMarker() {
        return (this.marker);
    }
    
    public Collection<Long> getHistory(Collection<Long> to_fill) {
        to_fill.addAll(this.history);
        return (to_fill);
    }
    
    // ----------------------------------------------------------------------------
    // START METHODS
    // ----------------------------------------------------------------------------

    /**
     * Main method for stop this ProfileMeasurement from recording time
     * 
     * @return this
     */

    public ProfileMeasurement start(long timestamp) {
        assert (this.marker == NULL_MARKER) : 
            String.format("Trying to start %s before it was stopped!", this.name);
        if (debug.val)
            LOG.debug(String.format("START %s", this));
        this.marker = timestamp;
        this.invocations++;
        if (this.start_observable != null)
            this.start_observable.notifyObservers(this);
        return (this);
    }

    public ProfileMeasurement start() {
        return (this.start(getTime()));
    }

    public boolean isStarted() {
        return (this.marker != NULL_MARKER);
    }

    public void addStartObserver(EventObserver<ProfileMeasurement> observer) {
        if (this.start_observable == null) {
            synchronized (this) {
                if (this.start_observable == null) {
                    this.start_observable = new EventObservable<ProfileMeasurement>();        
                }
            } // SYNCH
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
    public ProfileMeasurement stop(long timestamp) {
        if (this.reset) {
            this.reset = false;
            this.marker = NULL_MARKER;
            return (this);
        }
        if (debug.val)
            LOG.debug(String.format("STOP %s", this));
        assert (this.marker != NULL_MARKER) : 
            String.format("Trying to stop %s before it was started!", this.name);
        long added = (timestamp - this.marker);
        if (added < 0) {
            LOG.warn(String.format("Invalid stop timestamp for %s [timestamp=%d, marker=%d, added=%d]",
                                   this.name, timestamp, this.marker, added));
        } else {
            this.total_time += added;
            if (this.history != null) this.history.add(added);
        }
        this.marker = NULL_MARKER;
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
    
    public ProfileMeasurement stopIfStarted() {
        return this.stopIfStarted(getTime());
    }
    
    public ProfileMeasurement stopIfStarted(long timestamp) {
        if (this.isStarted()) this.stop(timestamp);
        return (this);
    }

    public void addStopObserver(EventObserver<ProfileMeasurement> observer) {
        if (this.stop_observable == null) {
            synchronized (this) {
                if (this.stop_observable == null) {
                    this.stop_observable = new EventObservable<ProfileMeasurement>();        
                }
            } // SYNCH
        }
        this.stop_observable.addObserver(observer);
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    public ProfileMeasurement restart() {
        long timestamp = ProfileMeasurement.getTime();
        this.stopIfStarted(timestamp).start(timestamp);
        return (this);
    }
    
    public ProfileMeasurement decrementTime(ProfileMeasurement other, boolean checkType) {
        assert(other != null);
        if (checkType) assert(this.name == other.name);
        this.total_time = Math.max(0, this.total_time - other.total_time);
        this.invocations = Math.max(0, this.invocations - other.invocations);
        return (this);
    }
    
    public ProfileMeasurement decrementTime(ProfileMeasurement other) {
        return (this.decrementTime(other, false));
    }

    public ProfileMeasurement appendTime(ProfileMeasurement other) {
        assert(other != null);
        this.total_time += other.total_time;
        this.marker = other.marker;
        this.invocations += other.invocations;
        return (this);
    }
    
    /**
     * Append the think time without locking the marker. This is the preferred
     * way to update the ProfileMeasurement when multiple threads are using it.
     * @param start
     * @param stop
     * @param invocations
     */
    public void appendTime(long start, long stop, int invocations) {
        assert(start >= 0);
        assert(stop >= 0);
        this.total_time += (stop - start);
        this.invocations += invocations;
    }

    /**
     * Append the think time without locking the marker. This is the preferred
     * way to update the ProfileMeasurement when multiple threads are using it.
     * @param start
     * @param stop
     */
    public void appendTime(long start, long stop) {
        this.appendTime(start, stop, 1);
    }

    /**
     * Append the think time without locking the marker. This is the preferred
     * way to update the ProfileMeasurement when multiple threads are using it.
     * @param start
     */
    public void appendTime(long start) {
        this.appendTime(start, getTime(), 1);
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

    // --------------------------------------------------------------------------------------------
    // DEBUG METHODS
    // --------------------------------------------------------------------------------------------
    
    @Override
    public String toString() {
        return (this.debug(false));
    }

    public String debug() {
        return this.debug(true);
    }

    private String debug(boolean verbose) {
        String prefix = this.name + "/" + this.hashCode();
        if (verbose) {
            return (String.format("%s[total=%d, marker=%s, invocations=%d, avg=%.2f ms]",
                                  prefix,
                                  this.total_time,
                                  this.marker,
                                  this.invocations,
                                  this.getAverageThinkTimeMS()));
        } else {
            return (prefix);
        }
    }

    // --------------------------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // --------------------------------------------------------------------------------------------

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        stringer.key("NAME").value(this.name);
        stringer.key("TIME").value(this.total_time);
        stringer.key("INVOCATIONS").value(this.invocations);
        if (this.history != null) {
            stringer.key("HISTORY").array();
            for (long val : this.history) {
                stringer.value(val);
            } // FOR
            stringer.endArray();
        }
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        this.name = json_object.getString("NAME");
        this.total_time = json_object.getLong("TIME");
        this.invocations = json_object.getInt("INVOCATIONS");
        if (json_object.has("HISTORY")) {
            this.history = null;
            this.enableHistoryTracking();
            JSONArray json_arr = json_object.getJSONArray("HISTORY");
            for (int i = 0, cnt = json_arr.length(); i < cnt; i++) {
                this.history.add(json_arr.getLong(i));
            } // FOR
        }
    }
}