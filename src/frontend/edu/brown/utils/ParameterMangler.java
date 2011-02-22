package edu.brown.utils;

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
            boolean is_array = catalog_param.getIsarray();
            this.param_isarray[catalog_param.getIndex()] = is_array;
            found_array = found_array || is_array;
        }
        this.has_arrays = found_array;
    }
    
    
    public Object[] convert(Object orig[]) {
        // Nothing!
        if (this.has_arrays == false) return (orig);
        
        Object cast_args[] = new Object[this.params.length];
        for (int i = 0; i < this.params.length; i++) {
            // Primitive Arrays! This is messed up in Java and why we're even here!
            if (this.param_isarray[i]) {
                Object inner[] = null;
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
                    }
                    case FLOAT: {
                        double arr[] = (double[])orig[i];
                        inner = new Object[arr.length];
                        for (int j = 0; j < arr.length; j++) {
                            inner[j] = arr[j];
                        } // FOR
                    }
                    default:
                        assert(false) : "Unhandled type " + this.param_types[i];
                } // SWITCH
                cast_args[i] = inner;
            } else {
                cast_args[i] = orig[i];
            }
        } // FOR
        return (cast_args);
    }
    
    
}
