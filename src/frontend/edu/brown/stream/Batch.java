package edu.brown.stream;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Batch {
    
    private long m_id;
    private long m_timestamp;
    
    private List<Tuple> m_tuples; 
    
    public Batch()
    {
        m_id = -1; // -1 is regarded as illegal id
        m_timestamp = -1; // illegal time
        m_tuples = new ArrayList<Tuple>();
    }
    
    public void setID(long id)
    {
        this.m_id = id;
    }
    
    public long getID()
    {
        return this.m_id;
    }
    
    public void setTimestamp(long timestamp)
    {
        this.m_timestamp = timestamp;
    }
    
    public long getTimestamp()
    {
        return this.m_timestamp;
    }

    public void addTuple(Tuple tuple)
    {
        m_tuples.add(tuple);        
    }
    
    public void addTupleCollection(Collection<Tuple> tuples)
    {
        m_tuples.addAll(tuples);
    }
    
    public List<Tuple> getTuples()
    {
        return m_tuples;
    }
    
    public int getSize()
    {
        return m_tuples.size();
    }
    
    public void reset()
    {
        m_id = -1;
        m_timestamp = -1;
        m_tuples.clear();
    }
    
    public JSONObject toJSONObject()
    {
        
        JSONObject jsonBatch = new JSONObject();
        try
        {
            // prepare ID
            jsonBatch.put("ID", this.m_id);
            // prepare timestamp
            jsonBatch.put("TIMESTAMP", this.m_timestamp);
            
            // prepare tuples
            JSONArray jsonTuples = new JSONArray();
            for(int index=0; index<m_tuples.size(); index++)
            {
                Tuple tuple = m_tuples.get(index);
                JSONObject jsonTuple = tuple.toJSONObject();
                jsonTuples.put(jsonTuple);
            }
            
            jsonBatch.put("TUPLES", jsonTuples);
            
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }        
        return jsonBatch;
    }
    
    public String toJSONString()
    {
        JSONObject jsonBatch = this.toJSONObject();
        
        return jsonBatch.toString();
    }
    
    public void fromJSONObject(JSONObject jsonBatch)
    {
        this.reset();
        
        try {
            //get id
            long batchid = jsonBatch.getLong("ID");
            this.m_id = batchid;
            //get timestamp
            this.m_timestamp = jsonBatch.getLong("TIMESTAMP");
            
            JSONArray jsonBatchTuples = jsonBatch.getJSONArray("TUPLES");
            
            JSONObject jsonTuple;
            for(int i = 0; i < jsonBatchTuples.length(); i++)
            {
                jsonTuple = jsonBatchTuples.getJSONObject(i);
                
                Tuple tuple = new Tuple();
                String[] fieldnames = JSONObject.getNames(jsonTuple);

                for(int index=0; index<fieldnames.length; index++)
                {
                    String fieldname = fieldnames[index];
                    Object fieldvalue = jsonTuple.get(fieldname);
                    
                    tuple.addField(fieldname, fieldvalue);
                }
                this.addTuple(tuple);
            }
            
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    public void fromJSONString(String strBatch)
    {
        try {
            JSONObject jsonBatch = new JSONObject(strBatch);
            
            this.fromJSONObject(jsonBatch);
            
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
}