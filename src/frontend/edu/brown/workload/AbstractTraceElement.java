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

import java.text.ParseException;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONStringer;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogUtil;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public abstract class AbstractTraceElement<T extends CatalogType> implements JSONString, Cloneable {
    /** java.util.logging logger. */
    protected static final Logger LOG = Logger.getLogger(AbstractTraceElement.class);
    
    public enum Members {
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
        // Output (optional)
        OUTPUT,
        // Weight (optional)
        WEIGHT,
    };
    
    protected Long start_timestamp;
    protected Long stop_timestamp; 
    protected Object params[];
    protected String catalog_item_name;
    protected boolean aborted = false;
    protected Object output[][][];
    protected VoltType output_types[][];
    protected short weight = 1;
    
    public AbstractTraceElement() {
        // Nothing to do...
    }
    
    public AbstractTraceElement(String catalog_item_name, Object params[]) {
        this.params = params;
        this.catalog_item_name = catalog_item_name;
        this.start_timestamp = System.nanoTime();
        //this.stop_timestamp = -1l;
    }
    
    public AbstractTraceElement(T catalog_item, Object params[]) {
        this(catalog_item.getName(), params);
    }
    
    @Override
    public AbstractTraceElement<T> clone() {
        AbstractTraceElement<T> clone = this.cloneImpl();
        clone.start_timestamp = this.start_timestamp;
        clone.stop_timestamp = this.stop_timestamp;
        clone.aborted = this.aborted;
        clone.output = this.output;
        clone.output_types = this.output_types;
        clone.weight = this.weight;
        return (clone);
    }
    
    protected abstract <X> X cloneImpl();
    
    public void setWeight(int weight) {
        this.weight = (short)weight;
    }
    public void incrementWeight(int delta) {
        this.weight += delta;
    }
    public int getWeight() {
        return (this.weight);
    }
    
    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "[" + this.catalog_item_name + "]");
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
    
    public void setTimestamps(Long start, Long stop) {
        this.start_timestamp = start;
        this.stop_timestamp = stop;
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
    
    public boolean hasOutput() {
        return (this.output != null);
    }
    public void setOutput(Object[][]...output) {
        if (output == null || output.length == 0) return;
        this.output = output;
        
        // Build Output Types
        this.output_types = new VoltType[this.output.length][];
        for (int i = 0; i < this.output.length; i++) {
            Object data[][] = this.output[i];
            if (data == null) continue;
            Integer missing = null;
            for (int j = 0; j < data.length; j++) {
                Object row[] = data[j];
                if (row == null) continue;
                if (missing == null) {
                    missing = row.length;
                    this.output_types[i] = new VoltType[row.length];
                }
                assert(missing != null);
                for (int k = 0; k < row.length; k++) {
                    if (this.output_types[i][k] != null) continue;
                    Object val = row[k];
                    if (val == null) continue;
                    this.output_types[i][k] = VoltType.typeFromClass(val.getClass());
                    missing--;
                } // FOR (columns)
                if (missing == 0) break;
            } // FOR (rows)
        } // FOR
    }
    public void setOutput(VoltTable...output) {
        if (output == null || output.length == 0) return;
        this.output = new Object[output.length][][];
        
        this.output_types = new VoltType[output.length][];
        for (int i = 0; i < this.output.length; i++) {
            VoltTable vt = output[i];
            if (vt == null) continue;
            
            // TYPES
            this.output_types[i] = new VoltType[vt.getColumnCount()];
            for (int k = 0; k < this.output_types[i].length; k++) {
                this.output_types[i][k] = vt.getColumnType(k);
            } // FOR
            
            // DATA
            this.output[i] = new Object[vt.getRowCount()][vt.getColumnCount()];
            int j = 0;
            while (vt.advanceRow()) {
                VoltTableRow row = vt.getRow();
                for (int k = 0; k < this.output[i][j].length; k++) {
                    this.output[i][j][k] = row.get(k); 
                } // FOR (columns)
                j++;
            } // WHILE (rows)
        } // FOR (tables)
    }
        
    public Object[][][] getOutput() {
        return (this.output);
    }
    public Object[][] getOutput(int idx) {
        return (this.output[idx]);
    }
    public VoltType[] getOutputTypes(int idx) {
        return (this.output_types[idx]);
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
        stringer.key(Members.NAME.name()).value(this.catalog_item_name);
        stringer.key(Members.START.name()).value(this.start_timestamp);
        stringer.key(Members.STOP.name()).value(this.stop_timestamp);
        stringer.key(Members.ABORTED.name()).value(this.aborted);
        
        // WEIGHT
        if (this.weight > 1) {
            stringer.key(Members.WEIGHT.name()).value(this.weight);
        }
          
        // Output Tables
        stringer.key(Members.OUTPUT.name()).array();
        if (this.output != null) {
            for (int i = 0; i < this.output.length; i++) {
                stringer.object();
                
                // COLUMN TYPES
                stringer.key("TYPES").array();
                VoltType types[] = this.output_types[i];
                if (types != null) {
                    for (int j = 0; j < types.length; j++) {
                        stringer.value((types[j] == null ? VoltType.NULL : types[j]).name()); 
                    } // FOR
                }
                stringer.endArray();
                
                // DATA
                stringer.key("DATA").array();
                Object data[][] = this.output[i];
                if (data != null) {
                    for (int j = 0; j < data.length; j++) {
                        Object row[] = data[j];
                        stringer.array();
                        for (int k = 0; k < row.length; k++) {
                            stringer.value(data[j][k]);
                        } // FOR
                        stringer.endArray();
                    } // FOR
                }
                stringer.endArray();
                
                stringer.endObject();
            } // FOR
        }
        stringer.endArray();
        
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
                
                // I couldn't think of a better way to do this dynamically..
                // It's not trivial to go from primitive arrays to object arrays
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
                            LOG.error("Failed to deserialize " + CatalogUtil.getDisplayName(catalog_param) + " [type=" + param_type + ", isarray=" + param_isarray + "]", ex);
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
                        throw new RuntimeException("Array parameter " + j + " for " + catalog_param + " is null");
                    }
                } // FOR
                this.params[i] = VoltTypeUtil.getPrimitiveArray(param_type, inner);
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
        this.start_timestamp = object.getLong(Members.START.name());
        if (!object.isNull(Members.STOP.name())) {
            this.stop_timestamp = object.getLong(Members.STOP.name());
        }
        this.catalog_item_name = object.getString(Members.NAME.name());
        this.aborted = object.getBoolean(Members.ABORTED.name());
        
        // WEIGHT
        if (object.has(Members.WEIGHT.name())) {
            this.weight = (short)object.getInt(Members.WEIGHT.name());
        }
        
        // OUTPUT
        JSONArray output_arr = null;
        try {
            output_arr = object.getJSONArray(Members.OUTPUT.name());
        } catch (JSONException ex) {
            // IGNORE
        }
        if (output_arr != null) {
            for (int i = 0, cnt = output_arr.length(); i < cnt; i++) {
                if (i == 0) {
                    this.output = new Object[cnt][][];
                    this.output_types = new VoltType[cnt][];
                }
                JSONObject inner = output_arr.getJSONObject(i);
                assert(inner != null);

                // TYPES
                JSONArray types_arr = inner.getJSONArray("TYPES");
                this.output_types[i] = new VoltType[types_arr.length()];
                for (int j = 0; j < this.output_types[i].length; j++) {
                    this.output_types[i][j] = VoltType.typeFromString(types_arr.getString(j));
                } // FOR
                
                // DATA
                JSONArray data_arr = inner.getJSONArray("DATA");
                this.output[i] = new Object[data_arr.length()][this.output_types[i].length];
                for (int j = 0; j < this.output[i].length; j++) {
                    JSONArray row_arr = data_arr.getJSONArray(j);
                    if (row_arr == null) continue;
                    for (int k = 0; k < this.output[i][j].length; k++) {
                        String val_str = row_arr.getString(k);
                        try {
                            this.output[i][j][k] = VoltTypeUtil.getObjectFromString(this.output_types[i][k], val_str);
                        } catch (ParseException ex) {
                            throw new RuntimeException(String.format("Failed to deserialize output %s [%d][%d][%d]", i, j, k));
                        }
                    } // FOR (columns)
                } // FOR (rows)
            } // FOR ( tables)
        }
    }
}