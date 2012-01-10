package edu.brown.hstore.conf;

import java.io.File;

import org.apache.log4j.Logger;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;


public abstract class HStoreConfUtil {
    private static final Logger LOG = Logger.getLogger(HStoreConfUtil.class);

    private static final String groups[] = { "global", "client", "site" };
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        
        // Default HStoreConf
        HStoreConf hstore_conf = HStoreConf.singleton(true);
        
        // Build Common
        File file = new File(args.getOptParam(0));
        assert(file.exists());
        StringBuilder sb = new StringBuilder();
        for (String group : groups) {
            sb.append(hstore_conf.makeBuildXML(group));
        } // FOR
        FileUtil.writeStringToFile(file, sb.toString());
        LOG.info("Updated " + file);
        
        
    }
    
}
