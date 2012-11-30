package edu.brown.api.results;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.client.ClientResponse;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class ResponseEntries implements JSONSerializable {
    
    public static class Entry implements Comparable<Entry> {
        public long timestamp;
        public int clientId;
        public boolean singlePartition;
        public int basePartition;
        public Status status;
        public int resultSize;
        public boolean exception;
        public long clusterRoundTrip;
        public long clientRoundTrip;
        public int restartCounter;

        private Entry() {
         // Needed for deserialization
        }
        public Entry(ClientResponse cr, int clientId, long timestamp) {
            this.clientId = clientId;
            this.timestamp = timestamp;
            this.singlePartition = cr.isSinglePartition();
            this.basePartition = cr.getBasePartition();
            this.status = cr.getStatus();
            this.resultSize = cr.getResultsSize();
            this.exception = (cr.getException() != null);
            this.clusterRoundTrip = cr.getClusterRoundtrip();
            this.clientRoundTrip = cr.getClientRoundtrip();
            this.restartCounter = cr.getRestartCounter();
        }
        @Override
        public int compareTo(Entry other) {
            if (this.timestamp != other.timestamp) {
                return (int)(this.timestamp - other.timestamp);
            }
            if (this.basePartition != other.basePartition) {
                return (this.basePartition - other.basePartition);
            }
            if (this.clusterRoundTrip != other.clusterRoundTrip) {
                return (int)(this.clusterRoundTrip - other.clusterRoundTrip);
            }
            if (this.clientRoundTrip != other.clientRoundTrip) {
                return (int)(this.clientRoundTrip - other.clientRoundTrip);
            }
            if (this.restartCounter != other.restartCounter) {
                return (int)(this.restartCounter - other.restartCounter);
            }
            if (this.resultSize != other.resultSize) {
                return (int)(this.resultSize - other.resultSize);
            }
            if (this.clientId != other.clientId) {
                return (int)(this.clientId - other.clientId);
            }
            if (this.status != other.status) {
                return (this.status.compareTo(other.status));
            }
            if (this.singlePartition != other.singlePartition) {
                return (this.singlePartition ? 1 : -1);
            }
            if (this.exception != other.exception) {
                return (this.exception ? 1 : -1);
            }
            return (0);
        }
    }
    
    private final List<Entry> entries = new ArrayList<Entry>(); 
    
    
    public ResponseEntries() {
        // Needed for deserialization
    }
    /**
     * Copy constructor. This is not a deep copy
     * @param other
     */
    public ResponseEntries(ResponseEntries other) {
        this.entries.addAll(other.entries);
    }
    
    public void add(ClientResponse cr, int clientId, long timestamp) {
        this.entries.add(new Entry(cr, clientId, timestamp));
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
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
        // I did it this way because way because I think it will be faster to parse 
        
        // TIMESTAMPS
        stringer.key("TIMESTAMPS").array();
        for (Entry e : this.entries)
            stringer.value(e.timestamp);
        stringer.endArray();
        
        // CLIENT IDS
        stringer.key("CLIENT").array();
        for (Entry e : this.entries)
            stringer.value(e.clientId);
        stringer.endArray();
        
        // SINGLE-PARTITION
        stringer.key("SINGLEP").array();
        for (Entry e : this.entries)
            stringer.value(e.singlePartition);
        stringer.endArray();
        
        // BASE-PARTITION
        stringer.key("BASEP").array();
        for (Entry e : this.entries)
            stringer.value(e.basePartition);
        stringer.endArray();
        
        // STATUS
        stringer.key("STATUS").array();
        for (Entry e : this.entries)
            stringer.value(e.status.ordinal());
        stringer.endArray();
        
        // RESULT-SIZE
        stringer.key("SIZE").array();
        for (Entry e : this.entries)
            stringer.value(e.resultSize);
        stringer.endArray();
        
        // EXCEPTION
        stringer.key("EXCEPTION").array();
        for (Entry e : this.entries)
            stringer.value(e.exception);
        stringer.endArray();
        
        // CLUSTER ROUND-TRIP
        stringer.key("CLUSTERRT").array();
        for (Entry e : this.entries)
            stringer.value(e.clusterRoundTrip);
        stringer.endArray();
        
        // CLIENT ROUND-TRIP
        stringer.key("CLIENTRT").array();
        for (Entry e : this.entries)
            stringer.value(e.clientRoundTrip);
        stringer.endArray();
        
        // RESTART COUNTER
        stringer.key("RESTARTS").array();
        for (Entry e : this.entries)
            stringer.value(e.restartCounter);
        stringer.endArray();
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONArray jsonArrays[] = {
                json_object.getJSONArray("TIMESTAMPS"),
                json_object.getJSONArray("SINGLEP"),
                json_object.getJSONArray("BASEP"),
                json_object.getJSONArray("STATUS"),
                json_object.getJSONArray("SIZE"),
                json_object.getJSONArray("EXCEPTION"),
                json_object.getJSONArray("CLUSTERRT"),
                json_object.getJSONArray("CLIENTRT"),
                json_object.getJSONArray("RESTARTS")
        };
        assert(jsonArrays.length == json_object.names().length()) :
                String.format("%d != %d", jsonArrays, json_object.names().length());
        int expected = -1;
        for (JSONArray arr : jsonArrays) {
            if (expected != -1)
                assert(expected == arr.length()) :
                    String.format("%d != %d", expected, arr.length());
            expected = arr.length();
        } // FOR
        
        Status statuses[] = Status.values();
        for (int i = 0; i < expected; i++) {
            Entry e = new Entry();
            int col = 0;
            e.timestamp = jsonArrays[col++].getLong(i);
            e.clientId = jsonArrays[col++].getInt(i);
            e.singlePartition = jsonArrays[col++].getBoolean(i);
            e.basePartition = jsonArrays[col++].getInt(i);
            e.status = statuses[jsonArrays[col++].getInt(i)];
            e.resultSize = jsonArrays[col++].getInt(i);
            e.exception = jsonArrays[col++].getBoolean(i);
            e.clusterRoundTrip = jsonArrays[col++].getLong(i);
            e.clientRoundTrip = jsonArrays[col++].getLong(i);
            e.restartCounter = jsonArrays[col++].getInt(i);
            this.entries.add(e);
        } // FOR
    }
}
