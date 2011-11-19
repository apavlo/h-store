package edu.brown.utils;

import java.util.Arrays;

import org.voltdb.VoltType;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

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

        this.params = catalog_proc.getParameters().toArray(new ProcParameter[0]);
        this.param_isarray = new boolean[this.params.length];
        this.param_types = new VoltType[this.params.length];
        boolean found_array = false;
        for (ProcParameter catalog_param : this.catalog_proc.getParameters()) {
            int i = catalog_param.getIndex();
            boolean is_array = catalog_param.getIsarray();
            found_array = found_array || is_array;
            
            this.param_isarray[i] = is_array;
            this.param_types[i] = VoltType.get(catalog_param.getType());
        } // FOR
        this.has_arrays = found_array;
    }
    
    public String toString(Object mangled[]) {
//        Object mangled[] = this.convert(args);
        
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mangled.length; i++) {
            sb.append(String.format("  [%02d] ", i));
            if (this.param_isarray[i]) {
                sb.append(Arrays.toString((Object[])mangled[i]));
            } else {
                sb.append(mangled[i]);
            }
            sb.append("\n");
        } // FOR
        return (sb.toString());
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
                cast_args[i] = orig[i];
            }
        } // FOR
        return (cast_args);
    }
    
    
}
