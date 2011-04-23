/***************************************************************************
 *   Copyright (C) 2009 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.workload;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import org.json.*;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogUtil;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public abstract class AbstractTraceElement<T extends CatalogType> implements JSONString {
    /** java.util.logging logger. */
    protected static final Logger LOG = Logger.getLogger(AbstractTraceElement.class.getName());
    
    private static AtomicLong NEXT_ID = new AtomicLong(0);
    
    public enum Members {
        // Unique AbstractTraceElement Id
        ID,
        // Catalog Name
        NAME,
        // Start Time (relative to start of trace)
        START,
        // Stop Time (relative to start of trace)
        STOP,
        // Element Parameters
        PARAMS,
        // Aborted (true/false)
        ABORTED,
    };
    
    protected long id;
    protected Long start_timestamp;
    protected Long stop_timestamp; 
    protected Object params[];
    protected String catalog_item_name;
    protected boolean aborted = false;
    
    public static void setStartingId(long id) {
        AbstractTraceElement.NEXT_ID.set(id);
    }
    
    public AbstractTraceElement() {
        // Nothing to do...
    }
    
    public AbstractTraceElement(T catalog_item, Object params[]) {
        // Important: We have to create a new unique id here. This is needed so that we can keep
        // the list of the trace elements as they are created by the system in order to play back
        // the trace by what really happened (as opposed to just getting the individual xact elements
        // one by one.
        this.id = NEXT_ID.getAndIncrement();
        this.params = params;
        this.catalog_item_name = catalog_item.getName();
        this.start_timestamp = System.nanoTime();
        //this.stop_timestamp = -1l;
    }
    
    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "[" + this.catalog_item_name + ":#" + this.id + "]");
    }
    
    public void stop() {
        this.stop_timestamp = System.nanoTime();
    }
    
    public void abort() {
        this.stop();
        this.aborted = true;
    }
    
    public boolean isStopped() {
        return (this.stop_timestamp != null);
    }
    
    public boolean isAborted() {
        return (this.aborted);
    }
    
    /**
     * Return the the TraceId for this element
     * @return
     */
    public long getId() {
        return this.id;
    }
    
    /**
     * @return the start_timestamp
     */
    public Long getStartTimestamp() {
        return this.start_timestamp;
    }

    /**
     * @return the stop_timestamp
     */
    public Long getStopTimestamp() {
        return this.stop_timestamp;
    }
    
    public String getCatalogItemName() {
        return this.catalog_item_name;
    }
    
    public abstract T getCatalogItem(Database catalog_db);
    
    public Boolean getAborted() {
        return aborted;
    }
    
    /**
     * Get the number of parameters that this trace element has
     * @return
     */
    public int getParamCount() {
        return (this.params.length);
    }
    
    /**
     * Get the parameters object array for this trace element
     * @return
     */
    public Object[] getParams() {
        return this.params;
    }

    /**
     * @return the params
     */
    @SuppressWarnings("unchecked")
    public <U> U getParam(int i) {
        return (U)this.params[i];
    }
    
    public void setParam(int i, Object value) {
        this.params[i] = value;
    }

    /**
     * 
     * @param catalog_db
     * @return
     */
    public abstract String debug(Database catalog_db);

    @Override
    public String toJSONString() {
        assert(false);
        // Can't be implemented since we always need to have a Database catalog object
        return null;
    }
    
    public String toJSONString(Database catalog_db) {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            this.toJSONString(stringer, catalog_db);
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return stringer.toString();
    }
    
    public void toJSONString(JSONStringer stringer, Database catalog_db) throws JSONException {
        stringer.key(Members.ID.name()).value(this.id);
        stringer.key(Members.NAME.name()).value(this.catalog_item_name);
        stringer.key(Members.START.name()).value(this.start_timestamp);
        stringer.key(Members.STOP.name()).value(this.stop_timestamp);
        stringer.key(Members.ABORTED.name()).value(this.aborted);
          
        // This doesn't work because CatalogType doesn't have have we need
        //assert(this.catalog_item.getField("parameters") != null);
        //CatalogMap<CatalogType> param_map = (CatalogMap<CatalogType>)this.catalog_item.getField("parameters");
        CatalogMap<? extends CatalogType> param_map = null;
        T catalog_item = this.getCatalogItem(catalog_db);
        if (catalog_item instanceof Procedure) {
            param_map = (CatalogMap<? extends CatalogType>)((Procedure)catalog_item).getParameters();
        } else {
            param_map = (CatalogMap<? extends CatalogType>)((Statement)catalog_item).getParameters();
        }
        stringer.key(Members.PARAMS.name()).array();
        for (int i = 0; i < this.params.length; i++) {
            CatalogType catalog_param = param_map.get(i);
            Object param_isarray = catalog_param.getField("isarray");
            if (param_isarray != null && (Boolean)param_isarray) {
                stringer.array();
                Class<?> type = params[i].getClass();
                assert(type.isArray());
                Class<?> dataType = type.getComponentType();
                //
                // I couldn't think of a better way to do this dynamically..
                // It's not trivial to go from primitive arrays to object arrays
                //
                if (dataType == Long.TYPE) {
                    for (Object value : (long[])this.params[i]) {
                        stringer.value(value);
                    } // FOR
                } else if (dataType == Integer.TYPE) {
                    for (Object value : (int[])this.params[i]) {
                        stringer.value(value);
                    } // FOR
                } else if (dataType == Short.TYPE) {
                    for (Object value : (short[])this.params[i]) {
                        stringer.value(value);
                    } // FOR
                } else if (dataType == Byte.TYPE) {
                    for (Object value : (byte[])this.params[i]) {
                        stringer.value(value);
                    } // FOR
                } else {
                    for (Object value : (Object[])this.params[i]) {
                        stringer.value(value);
                    } // FOR
                }
                stringer.endArray();
            } else {
                stringer.value(this.params[i]);
            }
        } // FOR
        stringer.endArray();
    }
    
    protected <U extends CatalogType> void paramsFromJSONObject(JSONObject object, CatalogMap<U> catalog_params, String param_field) throws Exception {
        assert(catalog_params != null);
        final Thread self = Thread.currentThread();
        JSONArray jsonParams = object.getJSONArray(Members.PARAMS.name());
        int num_params = catalog_params.size();
        this.params = new Object[num_params];
        for (int i = 0; i < num_params; i++) {
            // If we ran out of JSON parameters, then just throw in null
            if (i > jsonParams.length()) {
                this.params[i] = null;
                continue;
            }
            
            U catalog_param = catalog_params.get(i);
            VoltType param_type = VoltType.get(((Integer)catalog_param.getField(param_field)).byteValue());
            // if (param_type == VoltType.TINYINT) param_type = VoltType.INTEGER;
            if (param_type == null) {
                throw new Exception("Parameter type is null for " + catalog_param);
            }
            // We don't know whether we have a StmtParameter or a ProcParameter
            Object _isarray = catalog_param.getField("isarray");
            boolean param_isarray = (_isarray != null && (Boolean)_isarray);
            
            // HACK: If parameter says its an array, but JSON disagrees, then just
            // treat it is as a String. The Volt guys started using byte arrays instead of
            // strings but didn't add a byte VoltType
            JSONArray jsonInner = null;
            if (param_isarray) {
                if (jsonParams.isNull(i)) {
                    this.params[i] = new Object[0];
                    continue;
                } else {
                    try {
                        jsonInner = jsonParams.getJSONArray(i);
                    } catch (JSONException ex) {
                        if (param_type == VoltType.TINYINT) {
                            param_isarray = false;
                            param_type = VoltType.STRING;
                        } else {
                            LOG.error("Failed to deserialize " + CatalogUtil.getDisplayName(catalog_param) + " [trace_id=" + this.id + ", type=" + param_type + ", isarray=" + param_isarray + "]", ex);
                            throw ex;        
                        }
                    }
                }
            }
            if (param_isarray) {
                Object inner[] = new Object[jsonInner.length()];
                for (int j = 0; j < jsonInner.length(); j++) {
                    inner[j] = VoltTypeUtil.getObjectFromString(param_type, jsonInner.getString(j), self);
                    if (inner[j] == null) {
                        LOG.fatal("Array parameter " + j + " for " + catalog_param + " is null");
                        System.exit(1);
                    }
                } // FOR
                this.params[i] = inner;
            } else if (jsonParams.isNull(i)) {
                this.params[i] = null;
            } else {
                //System.err.println("[" + i + "] " + jsonParams.getString(i) + " (" + param_type + ")");
                try {
                    this.params[i] = VoltTypeUtil.getObjectFromString(param_type, jsonParams.getString(i), self);
                    if (this.params[i] == null) {
                        throw new Exception(catalog_param + " is null [" + param_type + "]");
                    }
                } catch (Exception ex) {
                    LOG.fatal("Failed to convert param '" + jsonParams.getString(i) + "' to " + param_type + " for " + catalog_param + " in " + CatalogUtil.getDisplayName(catalog_param.getParent()), ex);
                    throw ex;
                }
            }
        } // FOR
    }
    
    protected void fromJSONObject(JSONObject object, Database db) throws JSONException {
        this.id = object.getLong(Members.ID.name());
        this.start_timestamp = object.getLong(Members.START.name());
        if (!object.isNull(Members.STOP.name())) {
            this.stop_timestamp = object.getLong(Members.STOP.name());
        }
        this.catalog_item_name = object.getString(Members.NAME.name());
        this.aborted = object.getBoolean(Members.ABORTED.name());
    }
}