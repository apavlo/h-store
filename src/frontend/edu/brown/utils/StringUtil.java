/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.voltdb.client.Client;
import org.voltdb.utils.Pair;

import com.google.protobuf.ByteString;

/**
 * @author pavlo
 */
public abstract class StringUtil {

    public static final String SPACER = "   ";
    public static final String DOUBLE_LINE = StringUtil.repeat("=", 64) + "\n";
    public static final String SINGLE_LINE = StringUtil.repeat("-", 64) + "\n";

    private static final Pattern LINE_SPLIT = Pattern.compile("\n");
    private static final Pattern TITLE_SPLIT = Pattern.compile(" ");

    private static String CACHE_REPEAT_STR = null;
    private static Integer CACHE_REPEAT_SIZE = null;
    private static String CACHE_REPEAT_RESULT = null;

    private static final double BASE = 1024, KB = BASE, MB = KB * BASE, GB = MB * BASE;
    private static final DecimalFormat df = new DecimalFormat("#.##");

    private static final String HEADER_MARKER = "-";
    private static final int HEADER_LENGTH = 80;
    
    
    /**
     * http://ubuntuforums.org/showpost.php?p=10215516&postcount=5
     * 
     * @param bytes
     * @return
     */
    public static String formatSize(double bytes) {
        if (bytes >= GB) {
            return df.format(bytes / GB) + " GB";
        } else if (bytes >= MB) {
            return df.format(bytes / MB) + " MB";
        } else if (bytes >= KB) {
            return df.format(bytes / KB) + " KB";
        }
        return "" + (int) bytes + " bytes";
    }

    /**
     * @param str
     * @return
     */
    public static String[] splitLines(String str) {
        return (str != null ? LINE_SPLIT.split(str) : null);
    }

    public static String header(String msg) {
        return StringUtil.header(msg, HEADER_MARKER, HEADER_LENGTH);
    }

    public static String header(String msg, int length) {
        return StringUtil.header(msg, HEADER_MARKER, length);
    }
    
    public static String header(String msg, String marker) {
        return StringUtil.header(msg, marker, HEADER_LENGTH);
    }

    /**
     * Create a nicely format header string where the given message is surround
     * on both sides by the marker. Example: "---------- MSG ----------"
     * 
     * @param msg
     * @param marker
     * @param length
     * @return
     */
    public static String header(String msg, String marker, int length) {
        int msg_length = msg.length();
        length = Math.max(msg_length, length);
        int border_len = (length - msg_length - 2) / 2;
        String border = StringUtil.repeat(marker, border_len);
        boolean add_extra = (border_len + msg_length + 2 + 1 == length);
        return String.format("%s %s %s%s", border, msg, border, (add_extra ? marker : ""));
    }

    /**
     * Return the MD5 checksum of the given string
     * 
     * @param input
     * @return
     */
    public static String md5sum(String input) {
        return md5sum(input.getBytes());
    }

    /**
     * Return the MD5 checksum of the given ByteBuffer
     * @param input
     * @return
     */
    public static String md5sum(ByteBuffer bytes) {
        byte b[] = new byte[bytes.limit()];
        bytes.get(b);
        return md5sum(b);
    }
    
    /**
     * Return the MD5 checksum of the given byte array
     * @param input
     * @return
     */
    public static String md5sum(byte bytes[]) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("Unable to compute md5sum for string", ex);
        }
        assert (digest != null);
        digest.update(bytes);
        BigInteger hash = new BigInteger(1, digest.digest());
        return (hash.toString(16));
    }

    /**
     * Split the multi-lined strings into separate columns
     * @param strs
     * @return
     */
    public static String columns(Collection<String> strs) {
        return StringUtil.columns(strs.toArray(new String[0]));
    }
    
    /**
     * Split the multi-lined strings into separate columns
     * @param strs
     * @return
     */
    public static String columns(String... strs) {
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
     * Return key/value maps into a nicely formatted table Delimiter ":", No
     * UpperCase Keys, No Boxing
     * 
     * @param maps
     * @return
     */
    public static String formatMaps(Map<?, ?>... maps) {
        return (formatMaps(":", false, false, false, false, true, true, maps));
    }

    /**
     * Return key/value maps into a nicely formatted table using the given
     * delimiter No Uppercase Keys, No Boxing
     * 
     * @param delimiter
     * @param maps
     * @return
     */
    public static String formatMaps(String delimiter, Map<?, ?>... maps) {
        return (formatMaps(delimiter, false, false, false, false, true, true, maps));
    }

    /**
     * Return key/value maps into a nicely formatted table The maps are
     * displayed in order from first to last, and there will be a spacer created
     * between each map. The format for each record is:
     * <KEY><DELIMITER><SPACING><VALUE> If the delimiter is an equal sign, then
     * the format is: <KEY><SPACING><DELIMITER><VALUE>
     * 
     * @param delimiter
     * @param upper
     *            Upper-case all keys
     * @param box
     *            Box results
     * @param border_top
     *            TODO
     * @param border_bottom
     *            TODO
     * @param recursive
     *            TODO
     * @param first_element_title
     *            TODO
     * @param maps
     * @return
     */
    @SuppressWarnings("unchecked")
    public static String formatMaps(String delimiter, boolean upper, boolean box, boolean border_top, boolean border_bottom, boolean recursive, boolean first_element_title, Map<?, ?>... maps) {
        boolean need_divider = (maps.length > 1 || border_bottom || border_top);

        // Figure out the largest key size so we can get spacing right
        int max_key_size = 0;
        int max_title_size = 0;
        final Map<Object, String[]> map_keys[] = (Map<Object, String[]>[]) new Map[maps.length];
        final boolean map_titles[] = new boolean[maps.length];
        for (int i = 0; i < maps.length; i++) {
            Map<?, ?> m = maps[i];
            if (m == null)
                continue;
            Map<Object, String[]> keys = new HashMap<Object, String[]>();
            boolean first = true;
            for (Object k : m.keySet()) {
                String k_str[] = LINE_SPLIT.split(k != null ? k.toString() : "");
                keys.put(k, k_str);

                // If the first element has a null value, then we can let it be
                // the title for this map
                // It's length doesn't affect the other keys, but will affect
                // the total size of the map
                if (first && first_element_title && m.get(k) == null) {
                    for (String line : k_str) {
                        max_title_size = Math.max(max_title_size, line.length());
                    } // FOR
                    map_titles[i] = true;
                } else {
                    for (String line : k_str) {
                        max_key_size = Math.max(max_key_size, line.length());
                    } // FOR
                    if (first)
                        map_titles[i] = false;
                }
                first = false;
            } // FOR
            map_keys[i] = keys;
        } // FOR

        boolean equalsDelimiter = delimiter.equals("=");
        final String f = "%-" + (max_key_size + delimiter.length() + 1) + "s" + (equalsDelimiter ? "= " : "") + "%s\n";

        // Now make StringBuilder blocks for each map
        // We do it in this way so that we can get the max length of the values
        int max_value_size = 0;
        StringBuilder blocks[] = new StringBuilder[maps.length];
        for (int map_i = 0; map_i < maps.length; map_i++) {
            blocks[map_i] = new StringBuilder();
            Map<?, ?> m = maps[map_i];
            if (m == null)
                continue;
            Map<Object, String[]> keys = map_keys[map_i];

            boolean first = true;
            for (Entry<?, ?> e : m.entrySet()) {
                String key[] = keys.get(e.getKey());

                if (first && map_titles[map_i]) {
                    blocks[map_i].append(StringUtil.join("\n", key));
                    if (CollectionUtil.last(key).endsWith("\n") == false)
                        blocks[map_i].append("\n");

                } else {
                    Object v_obj = e.getValue();
                    String v = null;
                    if (recursive && v_obj instanceof Map<?, ?>) {
                        v = formatMaps(delimiter, upper, box, border_top, border_bottom, recursive, first_element_title, (Map<?, ?>) v_obj).trim();
                    } else if (key.length == 1 && key[0].trim().isEmpty() && v_obj == null) {
                        blocks[map_i].append("\n");
                        continue;
                    } else if (v_obj == null) {
                        v = "null";
                    } else {
                        v = v_obj.toString();
                    }

                    // If the key or value is multiple lines, format them
                    // nicely!
                    String value[] = LINE_SPLIT.split(v);
                    int total_lines = Math.max(key.length, value.length);
                    for (int line_i = 0; line_i < total_lines; line_i++) {
                        String k_line = (line_i < key.length ? key[line_i] : "");
                        if (upper)
                            k_line = k_line.toUpperCase();

                        String v_line = (line_i < value.length ? value[line_i] : "");

                        if (line_i == (key.length - 1) && (first == false || (first && v_line.isEmpty() == false))) {
                            if (equalsDelimiter == false && k_line.trim().isEmpty() == false)
                                k_line += ":";
                        }

                        blocks[map_i].append(String.format(f, k_line, v_line));
                        if (need_divider)
                            max_value_size = Math.max(max_value_size, v_line.length());
                    } // FOR
                    if (v.endsWith("\n"))
                        blocks[map_i].append("\n");
                }
                first = false;
            }
        } // FOR

        // Put it all together!
        // System.err.println("max_title_size=" + max_title_size +
        // ", max_key_size=" + max_key_size + ", max_value_size=" +
        // max_value_size + ", delimiter=" + delimiter.length());
        int total_width = Math.max(max_title_size, (max_key_size + max_value_size + delimiter.length())) + 1;
        String dividing_line = (need_divider ? repeat("-", total_width) : "");
        StringBuilder sb = null;
        if (maps.length == 1) {
            sb = blocks[0];
        } else {
            sb = new StringBuilder();
            for (int i = 0; i < maps.length; i++) {
                if (blocks[i].length() == 0)
                    continue;
                if (i != 0 && maps[i].size() > 0)
                    sb.append(dividing_line).append("\n");
                sb.append(blocks[i]);
            } // FOR
        }
        return (box ? StringUtil.box(sb.toString()) : (border_top ? dividing_line + "\n" : "") + sb.toString() + (border_bottom ? dividing_line : ""));
    }

    /**
     * @param maps
     * @return
     */
    public static String formatMapsBoxed(Map<?, ?>... maps) {
        return (formatMaps(":", false, true, false, false, true, true, maps));
    }

    /**
     * Returns the given string repeated the given # of times
     * 
     * @param str
     * @param size
     * @return
     */
    public static String repeat(String str, int size) {
        // We cache the last call in case they are making repeated calls for the
        // same thing
        if (CACHE_REPEAT_STR != null && CACHE_REPEAT_STR.equals(str) && CACHE_REPEAT_SIZE.equals(size)) {
            return (CACHE_REPEAT_RESULT);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++)
            sb.append(str);
        CACHE_REPEAT_RESULT = sb.toString();
        CACHE_REPEAT_STR = str;
        CACHE_REPEAT_SIZE = size;
        return (CACHE_REPEAT_RESULT);
    }

    /**
     * Make a box around some text. If str has multiple lines, then the box will
     * be the length of the longest string.
     * 
     * @param str
     * @return
     */
    public static String box(String str) {
        return (StringUtil.box(str, "*", null));
    }

    /**
     * Make a box around some text using the given marker character.
     * 
     * @param str
     * @param mark
     * @return
     */
    public static String box(String str, String mark) {
        return (StringUtil.box(str, mark, null));
    }

    /**
     * Create a box around some text
     * 
     * @param str
     * @param mark
     * @param max_len
     * @return
     */
    public static String box(String str, String mark, Integer max_len) {
        String lines[] = LINE_SPLIT.split(str);
        if (lines.length == 0)
            return ("");

        if (max_len == null) {
            for (String line : lines) {
                if (max_len == null || line.length() > max_len)
                    max_len = line.length();
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
     * 
     * @param str
     * @param prefix
     * @return
     */
    public static String prefix(String str, String prefix) {
        String lines[] = LINE_SPLIT.split(str);
        if (lines.length == 0)
            return ("");

        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(prefix).append(line).append("\n");
        } // FOR
        return (sb.toString());
    }

    /**
     * Abbreviate the given string. The last three chars will be periods
     * 
     * @param str
     * @param max
     * @return
     */
    public static String abbrv(String str, int max) {
        return (abbrv(str, max, true));
    }

    /**
     * Abbreviate the given string. If dots, then the last three chars will be
     * periods
     * 
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
     * 
     * @param string
     * @return
     */
    public static String title(String string) {
        return (StringUtil.title(string, false));
    }

    /**
     * Converts a string to title case (ala Python)
     * 
     * @param string
     * @param keep_upper
     *            If true, then any non-first character that is uppercase stays
     *            uppercase
     * @return
     */
    public static String title(String string, boolean keep_upper) {
        StringBuilder sb = new StringBuilder();
        String add = "";
        for (String part : TITLE_SPLIT.split(string)) {
            sb.append(add).append(part.substring(0, 1).toUpperCase());
            int len = part.length();
            if (len > 1) {
                if (keep_upper) {
                    for (int i = 1; i < len; i++) {
                        String c = part.substring(i, i + 1);
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
     * 
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
     * 
     * @param <T>
     * @param delimiter
     * @param items
     * @return
     */
    public static <T> String join(String delimiter, T... items) {
        return (join(delimiter, Arrays.asList(items)));
    }

    public static <T> String join(String delimiter, final Iterator<T> items) {
        return (join("", delimiter, CollectionUtil.iterable(items)));
    }

    /**
     * Python join()
     * 
     * @param delimiter
     * @param items
     * @return
     */
    public static String join(String delimiter, Iterable<?> items) {
        return (join("", delimiter, items));
    }

    /**
     * Python join() with optional prefix
     * 
     * @param prefix
     * @param delimiter
     * @param items
     * @return
     */
    public static String join(String prefix, String delimiter, Iterable<?> items) {
        if (items == null)
            return ("");
        if (prefix == null)
            prefix = "";

        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Object x : items) {
            if (prefix.isEmpty() == false)
                sb.append(prefix);
            sb.append(x != null ? x.toString() : x).append(delimiter);
            i++;
        }
        if (i == 0)
            return ("");
        sb.delete(sb.length() - delimiter.length(), sb.length());

        return sb.toString();
    }

    /**
     * Return the HOST+PORT pair extracted from a string with
     * "<hostname>:<portnum>"
     * 
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
     * Dump a ByteString to a text representation Copied from:
     * http://people.csail
     * .mit.edu/evanj/hg/index.cgi/javatxn/file/tip/src/edu/mit
     * /ExampleServer.java
     * 
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
