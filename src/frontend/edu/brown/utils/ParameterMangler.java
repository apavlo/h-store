package edu.brown.utils;

import java.util.Arrays;
import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;

/**
 * Hackishly convert primitive arrays into object arrays
 * @author pavlo
 */
public class ParameterMangler {
    
    private final Procedure catalog_proc;
    private final boolean has_arrays;
    private final VoltType param_types[];
    private final ProcParameter params[];
    private final boolean param_isarray[];

    public ParameterMangler(Procedure catalog_proc) {
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
                sb.append(Arrays.toString((Object[])mangled[i]));
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
    
    public Object[] convert(Object orig[]) {
        // Nothing!
        if (this.has_arrays == false) return (orig);
        
        Object cast_args[] = new Object[this.params.length];
        for (int i = 0; i < this.params.length; i++) {
            // Primitive Arrays! This is messed up in Java and why we're even here!
            VoltType vtype = this.param_types[i];
            if (this.param_isarray[i] && vtype != VoltType.STRING && vtype != VoltType.TIMESTAMP) {
                Object inner[] = null;
                try {
                    switch (this.param_types[i]) {
                        case TINYINT: {
                            byte arr[] = (byte[])orig[i];
                            inner = new Object[arr.length];
                            for (int j = 0; j < arr.length; j++) {
                                inner[j] = arr[j];
                            } // FOR    
                            break;
                        }
                        case SMALLINT: {
                            short arr[] = (short[])orig[i];
                            inner = new Object[arr.length];
                            for (int j = 0; j < arr.length; j++) {
                                inner[j] = arr[j];
                            } // FOR    
                            break;
                        }
                        case INTEGER: {
                            int arr[] = (int[])orig[i];
                            inner = new Object[arr.length];
                            for (int j = 0; j < arr.length; j++) {
                                inner[j] = arr[j];
                            } // FOR    
                            break;
                        }
                        case BIGINT: {
                            long arr[] = (long[])orig[i];
                            inner = new Object[arr.length];
                            for (int j = 0; j < arr.length; j++) {
                                inner[j] = arr[j];
                            } // FOR
                            break;
                        }
                        case FLOAT: {
                            float arr[] = (float[])orig[i];
                            inner = new Object[arr.length];
                            for (int j = 0; j < arr.length; j++) {
                                inner[j] = arr[j];
                            } // FOR
                            break;
                        }
                        default:
                            assert(false) : "Unhandled type " + this.param_types[i];
                    } // SWITCH
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to properly convert " + this.params[i].fullName(), ex);
                }
                cast_args[i] = inner;
            } else {
                assert(cast_args.length == orig.length) :
                    String.format("%s #%d :: cast[%d] != orig[%d]\nCAST:%s\nORIG:%s\nPARAMS:%s",
                                  catalog_proc, i, cast_args.length, orig.length,
                                  Arrays.toString(cast_args), Arrays.toString(orig), Arrays.toString(this.params));
                cast_args[i] = orig[i];
            }
        } // FOR
        return (cast_args);
    }
    
    
}
