/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.types.TimestampType;

public abstract class VoltTypeUtil {
    private static final Logger LOG = Logger.getLogger(VoltTypeUtil.class.getName());

    protected final static Random rand = new Random();
    protected static final Integer DATE_STOP = Math.round(System.currentTimeMillis() / 1000);
    protected static final Integer DATE_START = VoltTypeUtil.DATE_STOP - 153792000;
    
    public static final String DATE_FORMAT_PATTERNS[] = {
        TimestampType.STRING_FORMAT,
        "EEE MMM dd HH:mm:ss zzz yyyy",     // Wed Aug 03 00:00:00 EDT 2011
        "yyyy-MM-dd HH:mm:ss",              // 2010-03-05 20:14:15
        "yyyy-MM-dd",                       // 2010-03-05
        TimestampType.STRING_FORMAT + ".0", // 2010-03-05T20:14:16.000.0
    };
    private static final Map<Thread, SimpleDateFormat[]> CACHED_DATE_FORMATS = new HashMap<Thread, SimpleDateFormat[]>();

    public static Object getRandomValue(VoltType type) {
        return getRandomValue(type, VoltTypeUtil.rand);
    }
    
    public static Object getRandomValue(VoltType type, Random rand) {
        Object ret = null;
        switch (type) {
            // --------------------------------
            // INTEGERS
            // --------------------------------
            case TINYINT:
                ret = Integer.valueOf((byte) Math.abs(rand.nextInt() % 128));
                break;
            case SMALLINT:
                ret = Integer.valueOf((short) Math.abs(rand.nextInt() % 32768));
                break;
            case INTEGER:
                ret = Integer.valueOf(Math.abs(rand.nextInt() % 100000));
                break;
            case BIGINT:
                ret = Long.valueOf(Math.abs(rand.nextInt() % 100000));
                break;
            // --------------------------------
            // FLOATS
            // --------------------------------
            case FLOAT:
            case DECIMAL:
                ret = Double.valueOf(Math.abs(rand.nextDouble()));
                break;
            // --------------------------------
            // STRINGS
            // --------------------------------
            case STRING:
                //int size = rand.nextInt(31) + 1;
                int size = 124; 
                String ret_str = "";
                for (int ctr = 0; ctr < size; ctr++) {
                    char data = (char)(Math.abs(rand.nextInt()) % 128);
                    //
                    // Skip quotation marks
                    //
                    if (Character.isLetter(data) == false) {
                       ctr--;
                    } else {
                       ret_str += String.valueOf(data);
                    }
                 }
                ret = ret_str;
                break;
            // --------------------------------
            // TIMESTAMP
            // --------------------------------
            case TIMESTAMP: {
                int timestamp = rand.nextInt(VoltTypeUtil.DATE_STOP - VoltTypeUtil.DATE_START) + VoltTypeUtil.DATE_START;
                ret = new TimestampType(Long.valueOf(timestamp * 1000));
                break;
            }
            // --------------------------------
            // INVALID
            // --------------------------------
            default:
                LOG.severe("ERROR: Unable to generate random value for invalid ValueType '" + type + "'");
        } // SWITCH
        return (ret);
    }

    /*
     * Determine if a cast is allowable w/o loss of precision
     * for index key comparison. This is probably overly strict.
     */
    public static boolean isAllowableCastForKeyComparator(VoltType from, VoltType to) {
        // self to self cast is obviously fine.
        if (from == to)
            return true;

        // allow only float to float
        // allow only decimal to decimal
        // allow only string to string
        if (to == VoltType.STRING    ||
            from == VoltType.STRING  ||
            to == VoltType.DECIMAL   ||
            from == VoltType.DECIMAL ||
            to == VoltType.FLOAT     ||
            from == VoltType.FLOAT)
        {
            return from == to;
        }

        // disallow integers getting smaller
        if (from == VoltType.BIGINT) {
            if (to == VoltType.SMALLINT ||
                to == VoltType.TINYINT  ||
                to == VoltType.INTEGER)
                return false;
        }
        else if (from == VoltType.INTEGER) {
            if (to == VoltType.SMALLINT ||
                to == VoltType.TINYINT)
                return false;
        }
        else if (from == VoltType.SMALLINT) {
            if (to == VoltType.TINYINT)
                return false;
        }

        return true;
    }

    public static VoltType determineImplicitCasting(VoltType left, VoltType right) {
        //
        // Make sure both are valid
        //
        if (left == VoltType.INVALID || right == VoltType.INVALID) {
            throw new VoltTypeException("ERROR: Unable to determine cast type for '" + left + "' and '" + right + "' types");
        }
        // Check for NULL first, if either type is NULL the output is always NULL
        // XXX do we need to actually check for all NULL_foo types here?
        else if (left == VoltType.NULL || right == VoltType.NULL)
        {
            return VoltType.NULL;
        }
        //
        // No mixing of strings and numbers
        //
        else if ((left == VoltType.STRING && right != VoltType.STRING) ||
                (left != VoltType.STRING && right == VoltType.STRING))
        {
            throw new VoltTypeException("ERROR: Unable to determine cast type for '" +
                                        left + "' and '" + right + "' types");
        }

        // Allow promoting INTEGER types to DECIMAL.
        else if ((left == VoltType.DECIMAL || right == VoltType.DECIMAL) &&
                !(left.isExactNumeric() && right.isExactNumeric()))
        {
            throw new VoltTypeException("ERROR: Unable to determine cast type for '" +
                                        left + "' and '" + right + "' types");
        }
        //
        // The following list contains the rules that use for casting:
        //
        //    (1) If both types are a STRING, the output is always a STRING
        //        Note that up above we made sure that they do not mix strings and numbers
        //            Example: STRING + STRING -> STRING
        //    (2) If one type is a DECIMAL, the output is always a DECIMAL
        //        Note that above we made sure that DECIMAL only mixes with
        //        allowed types
        //    (3) Floating-point types take precedence over integers
        //            Example: FLOAT + INTEGER -> FLOAT
        //    (4) Specific types for floating-point and integer types take precedence
        //        over the more general types
        //            Example: MONEY + FLOAT -> MONEY
        //            Example: TIMESTAMP + INTEGER -> TIMESTAMP
        VoltType cast_order[] = { VoltType.STRING,
                                  VoltType.DECIMAL,
                                  VoltType.FLOAT,
                                  VoltType.TIMESTAMP,
                                  VoltType.BIGINT };
        for (VoltType cast_type : cast_order) {
            //
            // If any one of the types is the current cast type, we'll use that
            //
            if (left == cast_type || right == cast_type)
            {
                return cast_type;
            }
        }

        // If we have INT types smaller than BIGINT
        // promote the output up to BIGINT
        if ((left == VoltType.INTEGER || left == VoltType.SMALLINT || left == VoltType.TINYINT) &&
                (right == VoltType.INTEGER || right == VoltType.SMALLINT || right == VoltType.TINYINT))
        {
            return VoltType.BIGINT;
        }

        // If we get here, we couldn't figure out what to do
        throw new VoltTypeException("ERROR: Unable to determine cast type for '" +
                                    left + "' and '" + right + "' types");
    }

    /**
     * Some of our workloads (like TPC-E) will have commands and other crap in the #s that we need to strip out 
     * @param value
     * @return
     */
    private static String cleanNumberString(String value) {
        Matcher m = pattern_cleanNumberString.matcher(value);
        return (m.replaceAll(""));
    }
    private static final Pattern pattern_cleanNumberString = Pattern.compile("[,)]");
    
    /**
     * Returns a casted object of the input value string based on the given type
     * @throws ParseException
     */
    public static Object getObjectFromString(VoltType type, String value) throws ParseException {
        return (VoltTypeUtil.getObjectFromString(type, value, Thread.currentThread()));
    }
        
    /**
     * Returns a casted object of the input value string based on the given type
     * You can pass the current thread to get a faster access for the cached SimpleDateFormats
     * @param type
     * @param value
     * @param self
     * @return
     * @throws ParseException
     */
    public static Object getObjectFromString(VoltType type, String value, Thread self) throws ParseException {
        Object ret = null;
        switch (type) {
            // NOTE: All runtime integer parameters are actually Longs,so we will have problems
            // if we actually try to convert the object to one of the smaller numeric sizes

            // --------------------------------
            // INTEGERS
            // --------------------------------
            case TINYINT:
                //ret = Byte.valueOf(value);
                //break;
            case SMALLINT:
                //ret = Short.valueOf(value);
                //break;
            case INTEGER:
                ret = Integer.valueOf(value);
                break;
            case BIGINT:
                try {
                    ret = Long.valueOf(value);
                } catch (NumberFormatException ex) {
                    ret = Long.valueOf(cleanNumberString(value));
                }
                break;
            // --------------------------------
            // FLOATS
            // --------------------------------
            case FLOAT:
            case DECIMAL:
                try {
                    ret = Double.valueOf(value);
                } catch (NumberFormatException ex) {
                    ret = Double.valueOf(cleanNumberString(value));
                }
                break;
            // --------------------------------
            // STRINGS
            // --------------------------------
            case STRING:
                ret = value;
                break;
            // --------------------------------
            // TIMESTAMP
            // --------------------------------
            case TIMESTAMP: {
                Date date = null;
                int usecs = 0;
                if (value.isEmpty()) throw new RuntimeException("Empty " + type + " parameter value");
                
                // We have to do this because apparently SimpleDateFormat isn't thread safe
                SimpleDateFormat formats[] = VoltTypeUtil.CACHED_DATE_FORMATS.get(self);
                if (formats == null) {
                    formats = new SimpleDateFormat[VoltTypeUtil.DATE_FORMAT_PATTERNS.length];
                    for (int i = 0; i < formats.length; i++) {
                        formats[i] = new SimpleDateFormat(VoltTypeUtil.DATE_FORMAT_PATTERNS[i]);
                    } // FOR
                    VoltTypeUtil.CACHED_DATE_FORMATS.put(self, formats);
                }
                
                ParseException last_ex = null;
                for (int i = 0; i < formats.length; i++) {
                    try {
                        date = formats[i].parse(value);
                        // We need to get the last microseconds
                        if (i == 0) usecs = Integer.parseInt(value.substring(value.lastIndexOf('.')+1));
                    } catch (ParseException ex) {
                        last_ex = ex;
                    }
                    if (date != null) break;
                } // FOR
                if (date == null) throw last_ex;
                ret = new TimestampType((date.getTime() * 1000) + usecs);
                break;
            }
            // --------------------------------
            // BOOLEAN
            // --------------------------------
            case BOOLEAN:
                ret = Boolean.parseBoolean(value);
                break;

            // --------------------------------
            // NULL
            // --------------------------------
            case NULL:
                ret = null;
                break;
            // --------------------------------
            // INVALID
            // --------------------------------
            default: {
                String msg = "Unable to get object for value with invalid ValueType '" + type + "'";
                throw new ParseException(msg, 0);
                // LOG.severe(msg);
            }
        }
        return (ret);
    }
    
    public static Object getPrimitiveArray(VoltType type, Object objArray[]) {
        Object ret = null;
        switch (type) {
            // --------------------------------
            // INTEGERS
            // --------------------------------
            case TINYINT: {
                Byte valArray[] = new Byte[objArray.length];
                for (int i = 0; i < objArray.length; i++) {
                    valArray[i] = ((Integer)objArray[i]).byteValue();
                } // FOR
                ret = valArray;
                break;
            }
            case SMALLINT: {
                Short valArray[] = new Short[objArray.length];
                for (int i = 0; i < objArray.length; i++) {
                    valArray[i] = ((Integer)objArray[i]).shortValue();
                } // FOR
                ret = valArray;
                break;
            }
            case INTEGER: {
                Integer valArray[] = new Integer[objArray.length];
                for (int i = 0; i < objArray.length; i++) {
                    valArray[i] = (Integer)objArray[i];
                } // FOR
                ret = valArray;
                break;
            }
            case BIGINT: {
                Long valArray[] = new Long[objArray.length];
                for (int i = 0; i < objArray.length; i++) {
                    valArray[i] = (Long)objArray[i];
                } // FOR
                ret = valArray;
                break;
            }
            // --------------------------------
            // FLOATS
            // --------------------------------
            case FLOAT:
            case DECIMAL: {
                Double valArray[] = new Double[objArray.length];
                for (int i = 0; i < objArray.length; i++) {
                    valArray[i] = (Double)objArray[i];
                } // FOR
                ret = valArray;
                break;
            }
            // --------------------------------
            // STRINGS
            // --------------------------------
            case STRING: {
                String valArray[] = new String[objArray.length];
                for (int i = 0; i < objArray.length; i++) {
                    valArray[i] = (String)objArray[i];
                } // FOR
                ret = valArray;
                break;
            }
            // --------------------------------
            // TIMESTAMP
            // --------------------------------
            case TIMESTAMP: {
                TimestampType valArray[] = new TimestampType[objArray.length];
                for (int i = 0; i < objArray.length; i++) {
                    valArray[i] = (TimestampType)objArray[i];
                } // FOR
                ret = valArray;
                break;
            }
            // --------------------------------
            // BOOLEAN
            // --------------------------------
            case BOOLEAN: {
                Boolean valArray[] = new Boolean[objArray.length];
                for (int i = 0; i < objArray.length; i++) {
                    valArray[i] = (Boolean)objArray[i];
                } // FOR
                ret = valArray;
                break;
            }
            // --------------------------------
            // NULL
            // --------------------------------
            case NULL:
                ret = null;
                break;
            // --------------------------------
            // INVALID
            // --------------------------------
            default: {
                String msg = "Unable to get object for value with invalid ValueType '" + type + "'";
                throw new RuntimeException(msg);
            }
        } // SWITCH
        return (ret);
    }
}
