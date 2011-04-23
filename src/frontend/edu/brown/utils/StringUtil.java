package edu.brown.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.client.Client;
import org.voltdb.utils.Pair;

import com.google.protobuf.ByteString;

public abstract class StringUtil {

    public static final String SPACER       = "   ";
    public static final String DOUBLE_LINE  = "============================================================================\n";
    public static final String SINGLE_LINE  = "----------------------------------------------------------------------------\n";

    private static String CACHE_REPEAT_STR = null;
    private static Integer CACHE_REPEAT_SIZE = null;
    private static String CACHE_REPEAT_RESULT = null;
    
    private static Pattern LINE_SPLIT = Pattern.compile("\n");
    private static Pattern SPACE_SPLIT = Pattern.compile(" ");
    
    /**
     * Format the header + rows into a CSV
     * @param header
     * @param rows
     * @return
     */
    public static String csv(String header[], Object[]...rows) {
        return (StringUtil.table(",", false, false, header, rows));        
    }
    
    /**
     * Format the header + rows into a neat little table
     * @param header
     * @param rows
     * @return
     */
    public static String table(String header[], Object[]...rows) {
        return (StringUtil.table("  ", true, false, header, rows));
    }
    
    public static Map<String, String> tableMap(String header[], Object[]...rows) {
        String new_header[] = new String[header.length-1];
        for (int i = 0; i < new_header.length; i++) {
            new_header[i] = header[i+1];
        } // FOR
        Object new_rows[][] = new String[rows.length][];
        for (int i = 0; i < new_rows.length; i++) {
            Object row[] = rows[i];
            new_rows[i] = new String[row.length - 1];
            for (int j = 0; j < new_rows[i].length; j++) {
                new_rows[i][j] = row[j+1].toString();
            } // FOR
        }
        
        Map<String, String> m = new ListOrderedMap<String, String>();
        String lines[] = LINE_SPLIT.split(StringUtil.table(new_header, new_rows));
        for (int i = 0; i < lines.length; i++) {
            Object key = (i == 0 ? header[0] : rows[i-1][0]);  
            m.put((key != null ? key.toString() : null), lines[i]);
        } // FOR
        return (m);
        
    }
        
    /**
     * Format the header+rows into a table
     * @param delimiter the character to use in between cells
     * @param spacing whether to make the width of each column the size of the largest cell
     * @param quotes whether to surround each cell in quotation marks
     * @param header
     * @param rows
     * @return
     */
    public static String table(String delimiter, boolean spacing, boolean quotes, String header[], Object[]...rows) {
        // First we need to figure out the size for each column
        String col_formats[] = new String[header.length];
        for (int col_idx = 0; col_idx < col_formats.length; col_idx++) {
            String f = null;
            if (spacing) {
                int width = header[col_idx].length();
                for (int row_idx = 0; row_idx < rows.length; row_idx++) {
                    if (rows[row_idx][col_idx] != null) {
                        width = Math.max(width, rows[row_idx][col_idx].toString().length());
                    }
                } // FOR
                f = "%-" + width + "s";
            } else {
                f = "%s";
            }
            if (quotes) f = '"' + f + '"';
            col_formats[col_idx] = (col_idx > 0 ? delimiter : "") + f;
        } // FOR
        
        // Create header row
        StringBuilder sb = new StringBuilder();
        for (int col_idx = 0; col_idx < col_formats.length; col_idx++) {
            sb.append(String.format(col_formats[col_idx], header[col_idx]));
        } // FOR
        
        // Now dump out the table
        for (int row_idx = 0; row_idx < rows.length; row_idx++) {
            Object row[] = rows[row_idx];
            sb.append("\n");
            for (int col_idx = 0; col_idx < col_formats.length; col_idx++) {
                String cell = (row[col_idx] != null ? row[col_idx].toString() : "");
                sb.append(String.format(col_formats[col_idx], cell));    
            } // FOR
        } // FOR
        
        return (sb.toString());
    }
    
    /**
     * Split the multi-lined strings into separate columns
     * @param strs
     * @return
     */
    public static String columns(String...strs) {
        String lines[][] = new String[strs.length][];
        String prefixes[] = new String[strs.length];
        int max_length = 0;
        int max_lines = 0;
        
        for (int i = 0; i < strs.length; i++) {
            lines[i] = LINE_SPLIT.split(strs[i]);
            prefixes[i] = (i == 0 ? "" : " \u2503 ");
            for (String line : lines[i]) {
                max_length = Math.max(max_length, line.length());
            } // FOR
            max_lines = Math.max(max_lines, lines[i].length);
        } // FOR
        
        String f = "%-" + max_length + "s";
        StringBuilder sb = new StringBuilder();
        for (int ii = 0; ii < max_lines; ii++) {
            for (int i = 0; i < strs.length; i++) {
                String line = (ii >= lines[i].length ? "" : lines[i][ii]);
                sb.append(prefixes[i]).append(String.format(f, line));
            } // FOR
            sb.append("\n");
        } // FOR
        
        return (sb.toString());
    }
    
    /**
     * Return key/value maps into a nicely formatted table
     * Delimiter ":", No UpperCase Keys, No Boxing
     * @param maps
     * @return
     */
    public static String formatMaps(Map<?, ?>...maps) {
        return (formatMaps(":", false, false, maps));
    }
    
    /**
     * Return key/value maps into a nicely formatted table using the given delimiter
     * No Uppercase Keys, No Boxing
     * @param delimiter
     * @param maps
     * @return
     */
    public static String formatMaps(String delimiter, Map<?, ?>...maps) {
        return (formatMaps(delimiter, false, false, maps));
    }

    /**
     * Return key/value maps into a nicely formatted table
     * The maps are displayed in order from first to last, and there will be a spacer
     * created between each map. The format for each record is:
     * 
     * <KEY><DELIMITER><SPACING><VALUE>
     * 
     * If the delimiter is an equal sign, then the format is:
     * 
     *  <KEY><SPACING><DELIMITER><VALUE>
     * 
     * @param delimiter
     * @param upper Upper-case all keys
     * @param box Box results
     * @param maps
     * @return
     */
    public static String formatMaps(String delimiter, boolean upper, boolean box, Map<?, ?>...maps) {
        // Figure out the largest key size so we can get spacing right
        int max_key_size = 0;
        final Map<?, ?> map_keys[] = new Map<?, ?>[maps.length];
        for (int i = 0; i < maps.length; i++) {
            Map<?, ?> m = maps[i];
            Map<Object, String> keys = new HashMap<Object, String>();
            for (Object k : m.keySet()) {
                String k_str = k.toString();
                keys.put(k, k_str);
                max_key_size = Math.max(max_key_size, k_str.length());
            } // FOR
            map_keys[i] = keys;
        } // FOR
        
        boolean equalsDelimiter = delimiter.equals("=");
        final String f = "%-" + (max_key_size + 2) + "s" +
                         (equalsDelimiter ? "= " : "") +
                         "%s\n";
        
        // Now make StringBuilder blocks for each map
        // We do it in this way so that we can get the 
        int max_value_size = 0;
        StringBuilder blocks[] = new StringBuilder[maps.length];
        for (int i = 0; i < maps.length; i++) {
            Map<?, ?> m = maps[i];
            Map<?, ?> keys = map_keys[i];
            blocks[i] = new StringBuilder();
            
            for (Entry<?, ?> e : m.entrySet()) {
                String k = keys.get(e.getKey()).toString();
                String v = (e.getValue() != null ? e.getValue().toString() : "null");
                if (upper) k = k.toUpperCase();
                if (equalsDelimiter == false && k.isEmpty() == false) k += ":";
                
                // If the value is multiple lines, format them nicely!
                String lines[] = LINE_SPLIT.split(v);
                for (int line_i = 0; line_i < lines.length; line_i++) {
                    blocks[i].append(String.format(f, (line_i == 0 ? k : ""), lines[line_i]));
                    if (maps.length > 1) max_value_size = Math.max(max_value_size, lines[line_i].length());
                } // FOR
                if (v.endsWith("\n")) blocks[i].append("\n"); 
            }
        } // FOR
        
        // Put it all together!
        StringBuilder sb = null;
        if (maps.length == 1) {
            sb = blocks[0];
        } else {
            sb = new StringBuilder();
            for (int i = 0; i < maps.length; i++) {
                if (i != 0 && maps[i].size() > 0) sb.append(repeat("-", max_key_size + max_value_size + 2)).append("\n");
                sb.append(blocks[i]);
            } // FOR
        }
        return (box ? StringUtil.box(sb.toString()) : sb.toString());
    }

    /**
     * 
     * @param maps
     * @return
     */
    public static String formatMapsBoxed(Map<?, ?>...maps) {
        return (formatMaps(":", false, true, maps));
    }

    /**
     * Returns the given string repeated the given # of times
     * @param str
     * @param size
     * @return
     */
    public static String repeat(String str, int size) {
        // We cache the last call in case they are making repeated calls for the same thing
        if (CACHE_REPEAT_STR != null &&
            CACHE_REPEAT_STR.equals(str) &&
            CACHE_REPEAT_SIZE.equals(size)) {
            return (CACHE_REPEAT_RESULT);
        }
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) sb.append(str);
        CACHE_REPEAT_RESULT = sb.toString();
        CACHE_REPEAT_STR = str;
        CACHE_REPEAT_SIZE = size;
        return (CACHE_REPEAT_RESULT);
    }
    
    /**
     * Make a box around some text. If str has multiple lines, then the box will be the length
     * of the longest string.
     * @param str
     * @return
     */
    public static String box(String str) {
        return (StringUtil.box(str, "*", null));
    }

    /**
     * Make a box around some text using the given marker character.
     * @param str
     * @param mark
     * @return
     */
    public static String box(String str, String mark) {
        return (StringUtil.box(str, mark, null));
    }
    
    /**
     * Create a box around some text
     * @param str
     * @param mark
     * @param max_len
     * @return
     */
    public static String box(String str, String mark, Integer max_len) {
        String lines[] = LINE_SPLIT.split(str);
        if (lines.length == 0) return ("");
        
        if (max_len == null) {
            for (String line : lines) {
                if (max_len == null || line.length() > max_len) max_len = line.length();
            } // FOR
        }
        
        final String top_line = StringUtil.repeat(mark, max_len + 4); // padding
        final String f = "%s %-" + max_len + "s %s\n";
        
        StringBuilder sb = new StringBuilder();
        sb.append(top_line).append("\n");
        for (String line : lines) {
            sb.append(String.format(f, mark, line, mark));
        } // FOR
        sb.append(top_line);
        
        return (sb.toString());
    }

    /**
     * Append the prefix to the beginning of each line in str
     * @param str
     * @param prefix
     * @return
     */
    public static String prefix(String str, String prefix) {
        String lines[] = LINE_SPLIT.split(str);
        if (lines.length == 0) return ("");
        
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(prefix).append(line).append("\n");
        } // FOR
        return (sb.toString());
    }
    
    /**
     * Abbreviate the given string. The last three chars will be periods
     * @param str
     * @param max
     * @return
     */
    public static String abbrv(String str, int max) {
        return (abbrv(str, max, true));
    }

    /**
     * Abbreviate the given string. If dots, then the last three chars will be periods
     * @param str
     * @param max
     * @param dots
     * @return
     */
    public static String abbrv(String str, int max, boolean dots) {
        int len = str.length();
        String ret = null;
        if (len > max) {
            ret = (dots ? str.substring(0, max - 3) + "..." : str.substring(0, max));
        } else {
            ret = str;
        }
        return (ret);
    }
    
    /**
     * Converts a string to title case (ala Python)
     * @param string
     * @return
     */
    public static String title(String string) {
        return (StringUtil.title(string, false));
    }
    
    /**
     * Converts a string to title case (ala Python)
     * @param string
     * @param keep_upper If true, then any non-first character that is uppercase stays uppercase
     * @return
     */
    public static String title(String string, boolean keep_upper) {
        StringBuilder sb = new StringBuilder();
        String add = "";
        for (String part : SPACE_SPLIT.split(string)) {
            sb.append(add).append(part.substring(0, 1).toUpperCase());
            int len = part.length();
            if (len > 1) {
                if (keep_upper) {
                    for (int i = 1; i < len; i++) {
                        String c = part.substring(i, i+1);
                        String up = c.toUpperCase();
                        sb.append(c.equals(up) ? c : c.toLowerCase()); 
                    } // FOR
                } else {
                    sb.append(part.substring(1).toLowerCase());    
                }
            }
            add = " ";
        } // FOR
        return (sb.toString());
    }
    
    /**
     * Append SPACER to the front of each line in a string
     * @param str
     * @return
     */
    public static String addSpacers(String str) {
        StringBuilder sb = new StringBuilder();
        for (String line : LINE_SPLIT.split(str)) {
            sb.append(SPACER).append(line).append("\n");
        } // FOR
        return (sb.toString());
    }
    
    /**
     * Python join()
     * @param <T>
     * @param delimiter
     * @param items
     * @return
     */
    public static <T> String join(String delimiter, T...items) {
        return (join(delimiter, Arrays.asList(items)));
    }
    
    /**
     * Python join()
     * @param delimiter
     * @param items
     * @return
     */
    public static String join(String delimiter, Collection<?> items) {
        if (items.isEmpty()) return ("");
     
        StringBuilder sb = new StringBuilder();
        for (Object x : items)
            sb.append(x != null ? x.toString() : x).append(delimiter);
        sb.delete(sb.length() - delimiter.length(), sb.length());
     
        return sb.toString();
    }
    
    /**
     * Return the HOST+PORT pair extracted from a string with "<hostname>:<portnum>"
     * @param hostnport
     * @return
     */
    public static Pair<String, Integer> getHostPort(String hostnport) {
        return (getHostPort(hostnport, Client.VOLTDB_SERVER_PORT));
    }
    
    public static Pair<String, Integer> getHostPort(String hostnport, int port) {
        String host = hostnport;
        if (host.contains(":")) {
            String split[] = hostnport.split("\\:", 2);
            host = split[0];
            port = Integer.valueOf(split[1]);
        }
        return (Pair.of(host, port));
    }
    
    private static final char[] CHARACTERS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
    private static char nibbleToHexChar(int nibble) {
        assert 0 <= nibble && nibble < CHARACTERS.length;
        return CHARACTERS[nibble];
    }

    /**
     * Dump a ByteString to a text representation
     * Copied from: http://people.csail.mit.edu/evanj/hg/index.cgi/javatxn/file/tip/src/edu/mit/ExampleServer.java
     * @param bytes
     * @return
     */
    public static StringBuilder hexDump(ByteString bytes) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < bytes.size(); ++i) {
            if (i != 0 && i % 2 == 0) {
                out.append(' ');
            }
            byte b = bytes.byteAt(i);
            out.append(nibbleToHexChar((b >> 4) & 0xf));
            out.append(nibbleToHexChar(b & 0xf));
        }
        return out;
    } 


}
