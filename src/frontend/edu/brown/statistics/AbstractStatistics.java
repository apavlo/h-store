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
package edu.brown.statistics;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.catalog.CatalogKey;
import edu.brown.utils.StringUtil;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 * @param <T>
 */
public abstract class AbstractStatistics<T extends CatalogType> implements JSONString {
    protected static final Logger LOG = Logger.getLogger(AbstractStatistics.class);
    protected static final String DEBUG_SPACER = "  ";

    protected final String catalog_key;
    protected boolean has_preprocessed = false;

    /**
     * Constructor
     * 
     * @param catalog_key
     */
    public AbstractStatistics(String catalog_key) {
        this.catalog_key = catalog_key;
    }

    /**
     * Constructor
     * 
     * @param catalog_item
     */
    public AbstractStatistics(T catalog_item) {
        this.catalog_key = CatalogKey.createKey(catalog_item);
    }

    /**
     * Returns the Catalog key of the stats object
     * 
     * @return
     */
    public final String getCatalogKey() {
        return (this.catalog_key);
    }

    /**
     * @param catalog_db
     * @return
     */
    public abstract T getCatalogItem(Database catalog_db);

    /**
     * 
     */
    public abstract void preprocess(Database catalog_db);

    /**
     * @param xact
     * @throws Exception
     */
    public abstract void process(Database catalog_db, TransactionTrace xact) throws Exception;

    /**
     * @throws Exception
     */
    public abstract void postprocess(Database catalog_db) throws Exception;

    /**
     * Returns debugging information about this object
     * 
     * @return
     */
    public abstract String debug(Database catalog_db);

    protected String debug(Database catalog_db, Enum<?> elements[]) {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        try {
            Class<?> statsClass = this.getClass();
            for (Enum<?> element : elements) {
                Field field = statsClass.getDeclaredField(element.toString().toLowerCase());
                Object value = field.get(this);

                if (field.getClass().isAssignableFrom(SortedMap.class)) {
                    SortedMap<?, ?> orig_value = (SortedMap<?, ?>) value;
                    Map<String, Object> inner_m = new ListOrderedMap<String, Object>();
                    for (Object inner_key : orig_value.keySet()) {
                        Object inner_val = orig_value.get(inner_key);
                        if (inner_val instanceof AbstractStatistics<?>) {
                            inner_val = ((AbstractStatistics<?>) inner_val).debug(catalog_db);
                        }
                        inner_m.put(inner_key.toString(), inner_val);
                    } // FOR
                    value = StringUtil.formatMaps(inner_m);
                }
                m.put(element.toString(), value);
            } // FOR
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        return (String.format("%s\n%s", this.getCatalogItem(catalog_db), StringUtil.prefix(StringUtil.formatMaps(m), DEBUG_SPACER)));
    }

    /**
     * 
     */
    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            this.toJSONString(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return stringer.toString();
    }

    /**
     * @param stringer
     * @throws JSONException
     */
    public abstract void toJSONString(JSONStringer stringer) throws JSONException;

    /**
     * @param object
     * @throws JSONException
     */
    public abstract void fromJSONObject(JSONObject object, Database catalog_db) throws JSONException;

    /**
     * @param <T>
     * @param map
     * @param name
     * @param stringer
     * @throws JSONException
     */
    protected <U> void writeMap(SortedMap<U, ?> map, String name, JSONStringer stringer) throws JSONException {
        stringer.key(name).object();
        for (U key : map.keySet()) {
            String key_name = key.toString();
            if (key instanceof CatalogType) {
                key_name = ((CatalogType) key).getName();
            }
            stringer.key(key_name).value(map.get(key));
        } // FOR
        stringer.endObject();
    }

    /**
     * Reads a stored mapped from the JSON object and populates the data
     * structure
     * 
     * @param <U>
     * @param <V>
     * @param map
     * @param name
     * @param key_map
     * @param object
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    protected <U, V> void readMap(SortedMap<U, V> map, String name, Map<String, U> key_map, Class<?> value_class, JSONObject object) throws JSONException {
        map.clear();
        JSONObject jsonObject = object.getJSONObject(name);
        Iterator<String> keys = jsonObject.keys();
        boolean first = true;
        while (keys.hasNext()) {
            String key_name = keys.next();
            U key_object = null;
            V value = null;

            if (value_class.equals(Long.class)) {
                value = (V) new Long(jsonObject.getLong(key_name));
            } else {
                value = (V) jsonObject.get(key_name);
            }
            key_object = key_map.get(key_name);
            if (key_object == null) {
                LOG.warn("Failed to retrieve key object '" + key_name + "' for " + name);
                if (LOG.isDebugEnabled() && first) {
                    LOG.warn(jsonObject.toString(2));
                }
                first = false;
                continue;
            }
            map.put(key_object, value);
        } // FOR
        LOG.debug("Added " + map.size() + " values to " + name);
        return;
    }
}
