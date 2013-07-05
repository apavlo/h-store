/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;

/**
 * Hackishly convert primitive arrays into object arrays
 * 
 * @author pavlo
 */
public class ParameterMangler {

    private final Procedure catalog_proc;
    private final boolean has_arrays;
    private final VoltType param_types[];
    private final ProcParameter params[];
    private final boolean param_isarray[];

    private static final Map<Procedure, ParameterMangler> singletons = new HashMap<Procedure, ParameterMangler>();
    
    public static ParameterMangler singleton(Procedure catalog_proc) {
        ParameterMangler mangler = singletons.get(catalog_proc);
        if (mangler == null) {
            synchronized (ParameterMangler.class) {
                mangler = singletons.get(catalog_proc);
                if (mangler == null) {
                    mangler = new ParameterMangler(catalog_proc);
                    singletons.put(catalog_proc, mangler);
                }
            } // SYNCH
        }
        return (mangler);
    }
    
    private ParameterMangler(Procedure catalog_proc) {
        this.catalog_proc = catalog_proc;

        List<ProcParameter> catalog_params = CatalogUtil.getRegularProcParameters(catalog_proc);
        int num_params = catalog_params.size();
        this.params = new ProcParameter[num_params];
        this.param_isarray = new boolean[num_params];
        this.param_types = new VoltType[num_params];
        boolean found_array = false;
        for (int i = 0, cnt = catalog_params.size(); i < cnt; i++) {
            ProcParameter catalog_param = catalog_params.get(i);
            this.params[i] = catalog_param;
            this.param_isarray[i] = catalog_param.getIsarray();
            this.param_types[i] = VoltType.get(catalog_param.getType());
            found_array = found_array || this.param_isarray[i];
        } // FOR
        this.has_arrays = found_array;
    }

    public static String toString(Object mangled[], boolean is_array[]) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mangled.length; i++) {
            sb.append(String.format("  [%02d] ", i));
            if (is_array[i]) {
                Object inner[] = (Object[]) mangled[i];
                sb.append(String.format("%s / length=%d",
                                        Arrays.toString(inner), inner.length));
            } else {
                sb.append(mangled[i]);
            }
            sb.append("\n");
        } // FOR
        return (sb.toString());
    }

    public String toString(Object mangled[]) {
        return ParameterMangler.toString(mangled, this.param_isarray);
    }
    
    /**
     * Thread-safe
     * @param orig
     * @return
     */
    public Object[] convert(Object orig[]) {
        // Nothing!
        if (this.has_arrays == false)
            return (orig);

        Object cast_args[] = new Object[this.params.length];
        for (int i = 0; i < this.params.length; i++) {
            // Primitive Arrays! This is messed up in Java and why we're even here!
            VoltType vtype = this.param_types[i];
            if (this.param_isarray[i] && vtype != VoltType.STRING && vtype != VoltType.TIMESTAMP) {
                Object inner[] = null;
                try {
                    switch (this.param_types[i]) {
                        case TINYINT: {
                            if (orig[i] instanceof byte[]) {
                                byte arr[] = (byte[]) orig[i];
                                inner = new Byte[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (byte)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof short[]) {
                                short arr[] = (short[]) orig[i];
                                inner = new Byte[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (byte)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof int[]) {
                                int arr[] = (int[]) orig[i];
                                inner = new Byte[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (byte)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof long[]) {
                                long arr[] = (long[]) orig[i];
                                inner = new Byte[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (byte)arr[j];
                                } // FOR
                            }
                            else {
                                inner = new Byte[((Object[])orig[i]).length];
                                for (int j = 0; j < inner.length; j++) {
                                    inner[j] = ((Number)((Object[])orig[i])[j]).byteValue();
                                } // FOR
                            }
                            break;
                        }
                        case SMALLINT: {
                            if (orig[i] instanceof byte[]) {
                                byte arr[] = (byte[]) orig[i];
                                inner = new Short[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (short)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof short[]) {
                                short arr[] = (short[]) orig[i];
                                inner = new Short[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (short)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof int[]) {
                                int arr[] = (int[]) orig[i];
                                inner = new Short[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (short)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof long[]) {
                                long arr[] = (long[]) orig[i];
                                inner = new Short[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (short)arr[j];
                                } // FOR
                            }
                            else {
                                inner = new Short[((Object[])orig[i]).length];
                                for (int j = 0; j < inner.length; j++) {
                                    inner[j] = ((Number)((Object[])orig[i])[j]).shortValue();
                                } // FOR
                            }
                            break;
                        }
                        case INTEGER: {
                            if (orig[i] instanceof byte[]) {
                                byte arr[] = (byte[]) orig[i];
                                inner = new Integer[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (int)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof short[]) {
                                short arr[] = (short[]) orig[i];
                                inner = new Integer[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (int)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof int[]) {
                                int arr[] = (int[]) orig[i];
                                inner = new Integer[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (int)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof long[]) {
                                long arr[] = (long[]) orig[i];
                                inner = new Integer[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (int)arr[j];
                                } // FOR
                            }
                            else {
                                inner = new Integer[((Object[])orig[i]).length];
                                for (int j = 0; j < inner.length; j++) {
                                    inner[j] = ((Number)((Object[])orig[i])[j]).intValue();
                                } // FOR    
                            }
                            break;
                        }
                        case BIGINT: {
                            if (orig[i] instanceof byte[]) {
                                byte arr[] = (byte[]) orig[i];
                                inner = new Long[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (long)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof short[]) {
                                short arr[] = (short[]) orig[i];
                                inner = new Long[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (long)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof int[]) {
                                int arr[] = (int[]) orig[i];
                                inner = new Long[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (long)arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof long[]) {
                                long arr[] = (long[]) orig[i];
                                inner = new Long[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = (long)arr[j];
                                } // FOR
                            }
                            else {
                                inner = new Long[((Object[])orig[i]).length];
                                for (int j = 0; j < inner.length; j++) {
                                    inner[j] = ((Number)((Object[])orig[i])[j]).longValue();
                                } // FOR
                            }
                            break;
                        }
                        case FLOAT: {
                            if (orig[i] instanceof float[]) {
                                float arr[] = (float[]) orig[i];
                                inner = new Double[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = Double.valueOf(arr[j]);
                                } // FOR
                            }
                            else if (orig[i] instanceof double[]) {
                                double arr[] = (double[]) orig[i];
                                inner = new Double[arr.length];
                                for (int j = 0; j < arr.length; j++) {
                                    inner[j] = arr[j];
                                } // FOR
                            }
                            else if (orig[i] instanceof Float[]) {
                                inner = new Double[((Object[])orig[i]).length];
                                for (int j = 0; j < inner.length; j++) {
                                    inner[j] = ((Number)((Object[])orig[i])[j]).doubleValue();
                                } // FOR
                            }
                            else {
                                inner = new Double[((Object[])orig[i]).length];
                                for (int j = 0; j < inner.length; j++) {
                                    inner[j] = ((Number)((Object[])orig[i])[j]).doubleValue();
                                } // FOR
                            }
                            break;
                        }
                        default:
                            assert (false) : "Unhandled type " + this.param_types[i];
                    } // SWITCH
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to properly convert " + this.params[i].fullName(), ex);
                }
                cast_args[i] = inner;
            } else {
                assert (cast_args.length == orig.length) : String.format("%s #%d :: cast[%d] != orig[%d]\nCAST:%s\nORIG:%s\nPARAMS:%s", catalog_proc, i, cast_args.length, orig.length,
                        Arrays.toString(cast_args), Arrays.toString(orig), Arrays.toString(this.params));
                cast_args[i] = orig[i];
            }
        } // FOR
        return (cast_args);
    }

}
