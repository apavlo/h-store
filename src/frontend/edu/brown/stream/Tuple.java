package edu.brown.stream;

import java.util.Map;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

public class Tuple {
    
    private Map<String, Object> m_values;

    private String m_value;

    public Tuple()
    {
        m_values = new HashMap<String, Object>();
    }
    
    public Tuple(String value) {
        m_values = new HashMap<String, Object>();
        
        this.m_value = value;
    }
    
    public String getValue() {
        return m_value;
    }
    
    public void addField(String fieldname, Object fieldvalue)
    {
        m_values.put(fieldname, fieldvalue);
    }
    
    public Object getField(String fieldname)
    {
        return m_values.get(fieldname);
    }
    
    public int getFieldLength()
    {
        return m_values.size();
    }
    
    public void reset()
    {
        m_values.clear();
    }
    
    public JSONObject toJSONObject()
    {
        JSONObject jsonTuple = new JSONObject();
        try {
            for (Map.Entry<String, Object> entry : m_values.entrySet()) {
                String fieldname = entry.getKey();
                Object fieldvalue = entry.getValue();
                jsonTuple.put(fieldname, fieldvalue);
            }
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       
        return jsonTuple;
    }
    
    public void fromJSONObject(JSONObject jsonTuple)
    {
        this.reset();
        
        try {
            String[] fieldnames = JSONObject.getNames(jsonTuple);
            for(int index=0; index<fieldnames.length; index++)
            {
                String fieldname = fieldnames[index];
                Object fieldvalue = jsonTuple.get(fieldname);
                this.addField(fieldname, fieldvalue);
            }
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public String toJSONString()
    {
        JSONObject jsonTuple = this.toJSONObject();

        return jsonTuple.toString();
    }
    
    public void fromJSONString(String strTuple)
    {
        try {
            JSONObject jsonTuple = new JSONObject(strTuple);
            
            this.fromJSONObject(jsonTuple);
            
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
}
