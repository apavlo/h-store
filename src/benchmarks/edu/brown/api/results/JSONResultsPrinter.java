package edu.brown.api.results;

import org.json.JSONException;
import org.json.JSONObject;

import edu.brown.api.BenchmarkInterest;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONUtil;

/**
 * JSON Results Printer
 * @author pavlo
 */
public class JSONResultsPrinter implements BenchmarkInterest {
   
    private final HStoreConf hstore_conf;
    private boolean stop = false;
    
    public JSONResultsPrinter(HStoreConf hstore_conf) {
        this.hstore_conf = hstore_conf;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults results) {
        if (this.stop) return (null);
        
        FinalResult fr = new FinalResult(results);
        JSONObject json = null;
        try {
            json = new JSONObject(fr.toJSONString());
            if (hstore_conf.client.output_clients == false) {
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

    @Override
    public void benchmarkHasUpdated(BenchmarkResults currentResults) {
        // Nothing
    }

    @Override
    public void markEvictionStart() {
        // Nothing
    }

    @Override
    public void markEvictionStop() {
        // Nothing
    }

    @Override
    public void stop() {
        this.stop = true;
    }
}
