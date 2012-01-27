package edu.brown.hstore.conf;

import java.io.File;

import org.apache.log4j.Logger;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;


public abstract class HStoreConfUtil {
    private static final Logger LOG = Logger.getLogger(HStoreConfUtil.class);

    public static final String groups[] = { "global", "client", "site" };
    public static final String navigationLink = "\n[previous] [next]";
    
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
        
        // Conf Index
        file = new File("conf-index.html"); 
        String contents = "";
        for (String prefix : groups) {
            contents += hstore_conf.makeIndexHTML(prefix);
        } // FOR
        contents += navigationLink;
        FileUtil.writeStringToFile(file, contents);
        LOG.info("Updated " + file);
        
        // Group Conf Listings
        for (String prefix : groups) {
            file = new File("conf-" + prefix + ".html");
            contents = hstore_conf.makeHTML(prefix) + "\n" + navigationLink;
            FileUtil.writeStringToFile(file, contents);
            LOG.info("Updated " + file);
        } // FOR
    }

    
    
}
