package edu.brown.hstore.conf;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;

import edu.brown.hstore.conf.HStoreConf.Conf;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;

/**
 * Utility code for HStoreConf
 * @author pavlo
 */
public abstract class HStoreConfUtil {
    private static final Logger LOG = Logger.getLogger(HStoreConfUtil.class);

    public static final String groups[] = { "global", "client", "site" };
    public static final String navigationLink = "[previous] [next]";
    
    private static final Pattern REGEX_URL = Pattern.compile("(http[s]?://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|])");
    private static final String REGEX_URL_REPLACE = "<a href=\"$1\">$1</a>";
    
    private static final Pattern REGEX_CONFIG = Pattern.compile("\\$\\{([\\w]+)\\.([\\w\\_]+)\\}");
    private static final String REGEX_CONFIG_REPLACE = "<a href=\"/documentation/configuration/properties-file/$1#$2\" class=\"property\">$1.$2</a>";
    
    private static final Pattern REGEX_SYSPROC = Pattern.compile("\\@([A-Z][\\w]+)");
    private static final String REGEX_SYSPROC_REPLACE = "<a href=\"/documentation/system-procedures/%s\" class=\"sysproc\">@%s</a>";

    private static final String NO_PREFIX_MARKER = "*NONE*";
    
    private static final Map<String, String> PREFIX_LABELS = new HashMap<String, String>();
    static {
        // If the parameter doesn't have a prefix, then it will get this one
        PREFIX_LABELS.put(NO_PREFIX_MARKER, "System &amp; Environment");
        
        PREFIX_LABELS.put("exec", "Execution");
        PREFIX_LABELS.put("cpu", "CPU Affinity");
        PREFIX_LABELS.put("specexec", "Speculative Execution");
        PREFIX_LABELS.put("commandlog", "Command Logging");
        PREFIX_LABELS.put("anticache", "Anti-Caching");
        PREFIX_LABELS.put("mr", "MapReduce");
        PREFIX_LABELS.put("network", "Network");
        PREFIX_LABELS.put("txn", "Transaction");
        PREFIX_LABELS.put("queue", "Workload Queue");
        PREFIX_LABELS.put("mappings", "Parameter Mappings");
        PREFIX_LABELS.put("markov", "Transaction Prediction Models");
        PREFIX_LABELS.put("planner", "Runtime SQL Batch Planner");
        PREFIX_LABELS.put("coordinator", "Cluster Coordinator");
        PREFIX_LABELS.put("trace", "Workload Traces");
        PREFIX_LABELS.put("status", "Site Debug Status");
        PREFIX_LABELS.put("pool", "Object Pools");
        PREFIX_LABELS.put("log", "Debug Logging");
        PREFIX_LABELS.put("codespeed", "Codespeed API");
        PREFIX_LABELS.put("output", "Benchmark Output Control");
        PREFIX_LABELS.put("storage", "Internal Storage");
        PREFIX_LABELS.put("aries", "ARIES-based Recovery");
        PREFIX_LABELS.put("snapshot", "Snapshot Checkpoints");
    }
    
    // ----------------------------------------------------------------------------
    // HTML OUTPUT METHODS
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
        final StringBuilder sb = new StringBuilder().append("\n");
        final Map<Field, ConfigProperty> props = handle.getConfigProperties();
        
        // Parameters:
        //  (1) Row Type Name
        //  (2) Row Key
        //  (3) Row Value
        final String htmlRow = "<tr><td class=\"prop-%s\">%s:</td><td><tt>%s</tt></td>\n";
        
        // Parameters:
        //  (1) parameter
        //  (2) parameter
        //  (3) experimental
        //  (4) default value
        //  (5) description 
        final String template = "<a name=\"@@PROP@@\"></a>\n" +
                                "<li><tt class=\"property\">@@PROPFULL@@</tt>@@EXP@@ @@DEPRECATED@@\n" +
                                "<table>\n" +
                                String.format(htmlRow, "default", "Default", "@@DEFAULT@@") +
                                String.format(htmlRow, "type", "Permitted Tye", "@@TYPE@@") +
                                "@@VALUES_FULL@@" +
                                "@@DEPRECATED_FULL@@" +
                                "<tr><td colspan=\"2\">@@DESC@@</td></tr>\n" +
                                "</table></li>\n\n";
        // Configuraiton Section Header
        final String sectStart = "<ul class=\"property-list\">\n\n";
        final Pattern sectSplitter = Pattern.compile("_");

        // First split the fields based on their section names.
        // We will use a special marker to indicate that the field does not belong
        // to any section. We want that section to always come first.
        Map<String, List<Field>> sectFields = new LinkedHashMap<String, List<Field>>(); 
        sectFields.put(NO_PREFIX_MARKER, new ArrayList<Field>());
        for (Field f : props.keySet()) {
            String prefix = sectSplitter.split(f.getName(), 2)[0];
            if (PREFIX_LABELS.containsKey(prefix) == false) {
                sectFields.get(NO_PREFIX_MARKER).add(f);
            }
            else {
                if (sectFields.containsKey(prefix) == false) {
                    sectFields.put(prefix, new ArrayList<Field>());
                }
                sectFields.get(prefix).add(f);
            }
        } // FOR

        Map<String, String> values = new HashMap<String, String>();
        for (String sectPrefix : sectFields.keySet()) {
            String sectName = String.format("%s Parameters", PREFIX_LABELS.get(sectPrefix)); 
            // Wordpress adds extra <p> with this
            // sb.append(String.format("\n<!-- %s -->", sectName.toUpperCase()));
            if (sectPrefix != NO_PREFIX_MARKER) {
                sb.append(String.format("<a name=\"%s\"></a>", sectPrefix));
            }
            sb.append(String.format("<h2 class=\"property-list\">%s</h2>\n", sectName));
            sb.append(sectStart);
        
            for (Field f : sectFields.get(sectPrefix)) {
                ConfigProperty cp = props.get(f);
                values.clear();
    
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
                values.put("EXP", "");
                if (cp.experimental()) {
                    values.put("EXP", " <b class=\"experimental\">Experimental</b>");
                }
                
                // PERMITED VALUES (for Enums)
                values.put("VALUES_FULL", "");
                if (cp.enumOptions() != null && !cp.enumOptions().isEmpty()) {
                    Enum<?> permitted[] = conf.getEnumOptions(f, cp);
                    values.put("VALUES_FULL",
                               String.format(htmlRow, "values", "Permitted Values", Arrays.toString(permitted)) +
                               String.format(htmlRow, "values", "See Also", cp.enumOptions())
                    );
                }
                
                // DEPRECATED
                values.put("DEPRECATED", "");   
                values.put("DEPRECATED_FULL", "");
                if (cp.replacedBy() != null && !cp.replacedBy().isEmpty()) {
                    String replacedBy = "${" + cp.replacedBy() + "}";
                    Matcher m = REGEX_CONFIG.matcher(replacedBy);
                    if (m.find()) replacedBy = m.replaceAll(REGEX_CONFIG_REPLACE);
                    values.put("DEPRECATED", " <b class=\"deprecated\">Deprecated</b>");
                    values.put("DEPRECATED_FULL", String.format(htmlRow, "replaced", "Replaced By", replacedBy));
                }
                
                // DESC
                String desc = cp.description();
                
                // Create links to remote sites
                Matcher m = REGEX_URL.matcher(desc);
                if (m.find()) desc = m.replaceAll(REGEX_URL_REPLACE);
                
                // Create links to other parameters
                m = REGEX_CONFIG.matcher(desc);
                if (m.find()) desc = m.replaceAll(REGEX_CONFIG_REPLACE);
                
                // Create links to sysprocs
                m = REGEX_SYSPROC.matcher(desc);
                if (m.find()) {
                    String sysproc = m.group(1);
                    // Skip @ProcInfo
                    if (sysproc.equalsIgnoreCase(ProcInfo.class.getSimpleName()) == false) {
                        String replace = String.format(REGEX_SYSPROC_REPLACE, sysproc.toLowerCase(), sysproc);
                        desc = m.replaceAll(replace);
                    }
                }
                
                values.put("DESC", desc);
                
                // CREATE HTML FROM TEMPLATE
                String copy = template;
                for (String key : values.keySet()) {
                    copy = copy.replace("@@" + key.toUpperCase() + "@@", values.get(key));
                }
                sb.append(copy);
            } // FOR
            sb.append("</ul>");
        } // SECTION
        return (sb.toString().trim());
    }
    
    // ----------------------------------------------------------------------------
    // BUILD COMMON OUTPUT METHODS
    // ----------------------------------------------------------------------------
    
    public static String makeBuildCommonHeader() {
        StringBuilder sb = new StringBuilder();
        sb.append("<!-- DO NOT EDIT THIS FILE - it is machine generated -->\n");
        sb.append("<!-- See the documentation page on how to add new HStoreConf parameters -->\n");
        sb.append("<!-- http://hstore.cs.brown.edu/documentation/development/configuration-properties -->\n\n");
        return (sb.toString());
    }
    
    public static String makeBuildCommonXML(HStoreConf conf, String group) {
        final Conf handle = conf.getHandles().get(group);
        assert(handle != null);
        
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
    
    // ----------------------------------------------------------------------------
    // DEFAULT CONFIGURATION FILE OUTPUT METHODS
    // ----------------------------------------------------------------------------
    
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
        sb.append(makeBuildCommonHeader());
        for (String group : groups) {
            sb.append(makeBuildCommonXML(hstore_conf, group));
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
