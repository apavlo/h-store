package edu.brown.hstore.conf;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.brown.hstore.conf.HStoreConf.Conf;
import edu.brown.hstore.interfaces.ConfigProperty;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;


public abstract class HStoreConfUtil {
    private static final Logger LOG = Logger.getLogger(HStoreConfUtil.class);

    public static final String groups[] = { "global", "client", "site" };
    public static final String navigationLink = "[previous] [next]";
    
    static final Pattern REGEX_URL = Pattern.compile("(http[s]?://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|])");
    static final String REGEX_URL_REPLACE = "<a href=\"$1\">$1</a>";
    
    static final Pattern REGEX_CONFIG = Pattern.compile("\\$\\{([\\w]+)\\.([\\w\\_]+)\\}");
    static final String REGEX_CONFIG_REPLACE = "<a href=\"/documentation/configuration/properties-file/$1#$2\" class=\"property\">\\${$1.$2}</a>";

    // ----------------------------------------------------------------------------
    // OUTPUT METHODS
    // ----------------------------------------------------------------------------
    
    public static String makeIndexHTML(HStoreConf conf, String group) {
        final Conf handle = conf.getHandles().get(group);
        
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("<h2>%s Parameters</h2>\n<ul>\n", StringUtil.title(group)));
        
        for (Entry<Field, ConfigProperty> e : handle.getConfigProperties().entrySet()) {
            Field f = e.getKey();
            String entry = REGEX_CONFIG_REPLACE.replace("$1", group).replace("$2", f.getName()).replace("\\$", "$");
            sb.append("  <li>  ").append(entry).append("\n");
        } // FOR
        sb.append("</ul>\n\n");
        
        return (sb.toString());
    }
    
    public static String makeHTML(HStoreConf conf, String group) {
        final Conf handle = conf.getHandles().get(group);
        
        StringBuilder sb = new StringBuilder();
        sb.append("<ul class=\"property-list\">\n\n");
        
        // Parameters:
        //  (1) parameter
        //  (2) parameter
        //  (3) experimental
        //  (4) default value
        //  (5) description 
        final String template = "<a name=\"@@PROP@@\"></a>\n" +
                                "<li><tt class=\"property\">@@PROPFULL@@</tt>@@EXP@@ @@DEP@@\n" +
                                "<table>\n" +
                                "<tr><td class=\"prop-default\">Default:</td><td><tt>@@DEFAULT@@</tt></td>\n" +
                                "<tr><td class=\"prop-type\">Permitted Type:</td><td><tt>@@TYPE@@</tt></td>\n" +
                                "@@DEP_FULL@@" +
                                "<tr><td colspan=\"2\">@@DESC@@</td></tr>\n" +
                                "</table></li>\n\n";
        
        
        Map<String, String> values = new HashMap<String, String>();
        for (Entry<Field, ConfigProperty> e : handle.getConfigProperties().entrySet()) {
            Field f = e.getKey();
            ConfigProperty cp = e.getValue();

            // PROP
            values.put("PROP", f.getName());
            values.put("PROPFULL", String.format("%s.%s", group, f.getName()));
            
            // DEFAULT
            Object defaultValue = conf.getDefaultValue(f, cp);
            if (defaultValue != null) {
                String value = defaultValue.toString();
                Matcher m = REGEX_CONFIG.matcher(value);
                if (m.find()) value = m.replaceAll(REGEX_CONFIG_REPLACE);
                defaultValue = value;
            }
            values.put("DEFAULT", (defaultValue != null ? defaultValue.toString() : "null"));
            
            // TYPE
            values.put("TYPE", f.getType().getSimpleName().toLowerCase());
            
            // EXPERIMENTAL
            if (cp.experimental()) {
                values.put("EXP", " <b class=\"experimental\">Experimental</b>");
            } else {
                values.put("EXP", "");   
            }
            
            // DEPRECATED
            if (cp.replacedBy() != null && !cp.replacedBy().isEmpty()) {
                String replacedBy = "${" + cp.replacedBy() + "}";
                Matcher m = REGEX_CONFIG.matcher(replacedBy);
                if (m.find()) replacedBy = m.replaceAll(REGEX_CONFIG_REPLACE);
                values.put("DEP", " <b class=\"deprecated\">Deprecated</b>");
                values.put("DEP_FULL", "<tr><td class=\"prop-replaced\">Replaced By:</td><td><tt>" + replacedBy + "</tt></td>\n");
            } else {
                values.put("DEP", "");   
                values.put("DEP_FULL", "");
            }
            
            // DESC
            String desc = cp.description();
            
            // Create links to remote sites
            Matcher m = REGEX_URL.matcher(desc);
            if (m.find()) desc = m.replaceAll(REGEX_URL_REPLACE);
            
            // Create links to other parameters
            m = REGEX_CONFIG.matcher(desc);
            if (m.find()) desc = m.replaceAll(REGEX_CONFIG_REPLACE);
            values.put("DESC", desc);
            
            // CREATE HTML FROM TEMPLATE
            String copy = template;
            for (String key : values.keySet()) {
                copy = copy.replace("@@" + key.toUpperCase() + "@@", values.get(key));
            }
            sb.append(copy);
        } // FOR
        sb.append("</ul>\n");
        return (sb.toString());
    }
    
    public static String makeBuildXML(HStoreConf conf, String group) {
        final Conf handle = conf.getHandles().get(group);
        
        StringBuilder sb = new StringBuilder();
        sb.append("<!-- " + group.toUpperCase() + " -->\n");
        for (Entry<Field, ConfigProperty> e : handle.getConfigProperties().entrySet()) {
            Field f = e.getKey();
            ConfigProperty cp = e.getValue();
            
            if (cp.experimental()) {
                
            }
            String propName = String.format("%s.%s", group, f.getName());
            sb.append(String.format("<arg value=\"%s=${%s}\" />\n", propName, propName));
        } // FOR
        sb.append("\n");
        return (sb.toString());
    }
    
    /**
     * 
     */
    public static String makeDefaultConfig(HStoreConf conf) {
        return (makeConfig(conf, false));
    }
    
    public static String makeConfig(HStoreConf conf, boolean experimental) {
        StringBuilder sb = new StringBuilder();
        for (String group : conf.getHandles().keySet()) {
            Conf handle = conf.getHandles().get(group);

            sb.append("## ").append(StringUtil.repeat("-", 100)).append("\n")
              .append("## ").append(StringUtil.title(group)).append(" Parameters\n")
              .append("## ").append(StringUtil.repeat("-", 100)).append("\n\n");
            
            for (Entry<Field, ConfigProperty> e : handle.getConfigProperties().entrySet()) {
                Field f = e.getKey();
                ConfigProperty cp = e.getValue();
                if (cp.experimental() && experimental == false) continue;
                
                String key = String.format("%s.%s", group, f.getName());
                Object val = null;
                try {
                    val = f.get(handle);
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to get " + key, ex);
                }
                if (val instanceof String) {
                    String str = (String)val;
                    if (str.startsWith(conf.global.temp_dir)) {
                        val = str.replace(conf.global.temp_dir, "${global.temp_dir}");
                    } else if (str.equals(conf.global.defaulthost)) {
                        val = str.replace(conf.global.defaulthost, "${global.defaulthost}");
                    }
                }
                
                sb.append(String.format("%-50s= %s\n", key, val));
            } // FOR
            sb.append("\n");
        } // FOR
        return (sb.toString());
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        
        // Default HStoreConf
        HStoreConf hstore_conf = HStoreConf.singleton(true);
        
        // Build Common
        File file = new File(args.getOptParam(0));
        assert(file.exists());
        StringBuilder sb = new StringBuilder();
        for (String group : groups) {
            sb.append(makeBuildXML(hstore_conf, group));
        } // FOR
        FileUtil.writeStringToFile(file, sb.toString());
        LOG.info("Updated " + file);
        
        // Conf Index
        file = new File("conf-index.html"); 
        String contents = navigationLink + "\n\n";
        for (String prefix : groups) {
            contents += makeIndexHTML(hstore_conf, prefix);
        } // FOR
        contents += "\n" + navigationLink;
        FileUtil.writeStringToFile(file, contents);
        LOG.info("Updated " + file);
        
        // Group Conf Listings
        for (String prefix : groups) {
            file = new File("conf-" + prefix + ".html");
            contents = navigationLink + "\n" +
                       makeHTML(hstore_conf, prefix) + "\n\n" + 
                       navigationLink;
            FileUtil.writeStringToFile(file, contents);
            LOG.info("Updated " + file);
        } // FOR
    }

    
    
}
