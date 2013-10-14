package edu.brown.hstore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility class for creating the command-line arguments needed
 * to launch various components of H-Store
 * @author pavlo
 */
public abstract class HStoreLauncher {

//    public static String[] createHStoreSiteCommand(String projectName,
//                                                   File hstore_conf_path,
//                                                   int site_id,
//                                                   Map<String, String> confParams) {
//        
//        Map<String, String> copy = new HashMap<String, String>(confParams); 
//        copy.put("conf", hstore_conf_path.getAbsolutePath());
//        copy.put("project", projectName);
//        
//        
//        List<String> siteCommand = new ArrayList<String>();
//        siteCommand.add("ant");
//        siteCommand.add("hstore-site");
//        siteCommand.add("-Djar=" + projectJar);
//        
//        // Be sure to include our HStoreConf parameters
//        for (Entry<String, String> e : confParams.entrySet()) {
//            siteCommand.add(String.format("-D%s=%s", e.getKey(), e.getValue()));
//        }
//        
//        // HACK: Put the site id last so that we can easily find it
//        siteCommand.add("-Dsite.id=" + site_id);
//        
//        return (siteCommand.toArray(new String[0]));
//    }
    
    public static String[] createHStoreSiteCommand(File projectJar, int site_id, Map<String, String> confParams) {
        List<String> siteCommand = new ArrayList<String>();
        siteCommand.add("ant");
        siteCommand.add("hstore-site");
        siteCommand.add("-Djar=" + projectJar);
        
        // Be sure to include our HStoreConf parameters
        for (Entry<String, String> e : confParams.entrySet()) {
            siteCommand.add(String.format("-D%s=%s", e.getKey(), e.getValue()));
        }
        
        // HACK: Put the site id last so that we can easily find it
        siteCommand.add("-Dsite.id=" + site_id);
        
        return (siteCommand.toArray(new String[0]));
    }
    
    
}
