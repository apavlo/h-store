package edu.brown.utils;

import java.io.IOException;

import org.json.*;
import org.voltdb.catalog.Database;

public interface JSONSerializable extends JSONString {
    public void save(String output_path) throws IOException;
    public void load(String input_path, Database catalog_db) throws IOException;
    public void toJSON(JSONStringer stringer) throws JSONException;
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException;
}
