package edu.brown.api.results;

import org.json.JSONException;
import org.json.JSONObject;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public class JSONResultsPrinter extends ResultsPrinter {
   
    public JSONResultsPrinter(HStoreConf hstore_conf) {
        super(hstore_conf);
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults results) {
        if (this.output_basepartitions) {
            System.out.print(StringUtil.SINGLE_LINE);
            System.out.println("Base Partition Distribution:\n" + results.getBasePartitions());
            System.out.print(StringUtil.SINGLE_LINE);
        }
        if (this.output_responses) {
            System.out.print(StringUtil.SINGLE_LINE);
            System.out.println("Client Response Statuses:\n" + results.getResponseStatuses());
            System.out.print(StringUtil.SINGLE_LINE);
        }
        
        FinalResult fr = new FinalResult(results);
        JSONObject json = null;
        try {
            json = new JSONObject(fr.toJSONString());
            if (output_clients == false) {
                for (String key : CollectionUtil.iterable(json.keys())) {
                    if (key.toLowerCase().startsWith("client")) {
                        json.remove(key);        
                    }
                } // FOR
            }
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
        return "<json>\n" + JSONUtil.format(json) + "\n</json>";
    }
}
