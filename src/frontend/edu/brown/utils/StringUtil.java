package edu.brown.utils;

import java.util.Arrays;
import java.util.Collection;

public abstract class StringUtil {

    public static final String SPACER       = "   ";
    public static final String DOUBLE_LINE  = "============================================================================\n";
    public static final String SINGLE_LINE  = "----------------------------------------------------------------------------\n";

    private static String CACHE_REPEAT_STR = null;
    private static Integer CACHE_REPEAT_SIZE = null;
    private static String CACHE_REPEAT_RESULT = null;
    
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
        StringBuilder sb = new StringBuilder();
        String add = "";
        for (String part : string.split(" ")) {
            sb.append(add).append(part.substring(0, 1).toUpperCase());
            if (part.length() > 1) sb.append(part.substring(1).toLowerCase());
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
        for (String line : str.split("\n")) {
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
            sb.append(x.toString()).append(delimiter);
        sb.delete(sb.length() - delimiter.length(), sb.length());
     
        return sb.toString();
    }

}
