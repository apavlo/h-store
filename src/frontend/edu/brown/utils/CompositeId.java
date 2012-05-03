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
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

/**
 * Pack multiple values into a single long using bit-shifting
 * @author pavlo
 */
public abstract class CompositeId implements Comparable<CompositeId>, JSONSerializable {
    
    private transient int hashCode = -1;
    
    protected static final long[] compositeBitsPreCompute(int offset_bits[]) {
        long pows[] = new long[offset_bits.length];
        for (int i = 0; i < offset_bits.length; i++) {
            pows[i] = (long)(Math.pow(2, offset_bits[i]) - 1l);
        } // FOR
        return (pows);
    }
    
    protected final long encode(int offset_bits[], long offset_pows[]) {
        long values[] = this.toArray();
        assert(values.length == offset_bits.length);
        long id = 0;
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            long max_value = offset_pows[i];

            assert(values[i] >= 0) :
                String.format("%s value at position %d is %d %s",
                              this.getClass().getSimpleName(), i, values[i], Arrays.toString(values));
            assert(values[i] < max_value) :
                String.format("%s value at position %d is %d. Max value is %d\n%s",
                              this.getClass().getSimpleName(), i, values[i], max_value, this);
            
            id = (i == 0 ? values[i] : id | values[i]<<offset);
            offset += offset_bits[i];
        } // FOR
        this.hashCode = (int)(id ^ (id >>> 32)); // From Long.hashCode()
        return (id);
    }
    
    protected final long[] decode(long composite_id, int offset_bits[], long offset_pows[]) {
        long values[] = new long[offset_bits.length];
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = (composite_id>>offset & offset_pows[i]);
            offset += offset_bits[i];
        } // FOR
        return (values);
    }
    
    public abstract long encode();
    public abstract void decode(long composite_id);
    public abstract long[] toArray();
    
    @Override
    public int compareTo(CompositeId o) {
        return Math.abs(this.hashCode()) - Math.abs(o.hashCode());
    }
    
    @Override
    public int hashCode() {
        if (this.hashCode == -1) {
            this.encode();
            assert(this.hashCode != -1);
        }
        return (this.hashCode);
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
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
        stringer.key("ID").value(this.encode());
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        this.decode(json_object.getLong("ID"));
    }
}
