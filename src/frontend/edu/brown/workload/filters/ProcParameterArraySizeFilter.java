package edu.brown.workload.filters;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.types.ExpressionType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ClassUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * Filters out transactions based on the number of elements in a particular
 * ProcParameter that is an array
 * @author pavlo
 */
public class ProcParameterArraySizeFilter extends Filter {
    
    private final int param_idx;
    private final int param_size;
    private final ExpressionType exp_type;

    /**
     * Constructor
     * @param param_idx
     * @param size
     * @param exp_type
     */
    public ProcParameterArraySizeFilter(int param_idx, int size, ExpressionType exp_type) {
        super();
        this.param_idx = param_idx;
        this.param_size = size;
        this.exp_type = exp_type;
        assert(this.exp_type.name().startsWith("COMPARE_")) : "Invalid ExpressionType " + exp_type;
    }
    
    public ProcParameterArraySizeFilter(ProcParameter catalog_param, int size, ExpressionType exp_type) {
        this(catalog_param.getIndex(), size, exp_type);
        assert(catalog_param.getIsarray()) : CatalogUtil.getDisplayName(catalog_param, true) + " is not an array";
    }
    
    @Override
    protected void resetImpl() {
        // Ignore...
    }
    
    @Override
    protected FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
        if (element instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)element;
            // Procedure catalog_proc = xact.getCatalogItem(this.catalog_db);
            
            assert(xact.getParamCount() > this.param_idx) :
                xact + " only has " + xact.getParamCount() + " parameters but we need #" + this.param_idx;
            Object param = xact.getParam(this.param_idx);
            assert(ClassUtil.isArray(param)) :
                "Parameter #" + this.param_idx + " for " + xact + " is not an array";
            Object params_arr[] = (Object[])param;
            
            boolean allow = false;
            switch (this.exp_type) {
                case COMPARE_EQUAL:
                    allow = (params_arr.length == this.param_size);
                    break;
                case COMPARE_NOTEQUAL:
                    allow = (params_arr.length != this.param_size);
                    break;
                case COMPARE_GREATERTHAN:
                    allow = (params_arr.length > this.param_size);
                    break;
                case COMPARE_GREATERTHANOREQUALTO:
                    allow = (params_arr.length >= this.param_size);
                    break;
                case COMPARE_LESSTHAN:
                    allow = (params_arr.length < this.param_size);
                    break;
                case COMPARE_LESSTHANOREQUALTO:
                    allow = (params_arr.length <= this.param_size);
                    break;
                default:
                    assert(false) : "Invalid ExpressionType " + this.exp_type;
            } // SWITCH
            return (allow ? FilterResult.ALLOW : FilterResult.SKIP);
        }
        return FilterResult.ALLOW;
    }
    
    @Override
    public String debugImpl() {
        return (this.getClass().getSimpleName() + "[" +
                "param_idx=" + param_idx + ", " +
                "size=" + this.param_size + ", " +
                "exp=" + this.exp_type + "]");
    }

}
