package edu.brown.expressions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltdb.expressions.*;
import org.voltdb.types.ExpressionType;

/**
 * @author pavlo
 */
public abstract class ExpressionUtil {

    public static final Map<ExpressionType, String> EXPRESSION_STRING = new HashMap<ExpressionType, String>();
    static {
        EXPRESSION_STRING.put(ExpressionType.COMPARE_EQUAL, "=");
        EXPRESSION_STRING.put(ExpressionType.COMPARE_NOTEQUAL, "!=");
        EXPRESSION_STRING.put(ExpressionType.COMPARE_LESSTHAN, "<");
        EXPRESSION_STRING.put(ExpressionType.COMPARE_LESSTHANOREQUALTO, "<=");
        EXPRESSION_STRING.put(ExpressionType.COMPARE_GREATERTHAN, ">");
        EXPRESSION_STRING.put(ExpressionType.COMPARE_GREATERTHANOREQUALTO, ">");
        EXPRESSION_STRING.put(ExpressionType.COMPARE_LIKE, "LIKE");
        EXPRESSION_STRING.put(ExpressionType.COMPARE_IN, "IN");
    }

    /**
     * Get all of the ExpressionTypes used in the tree below the given root
     * @param root
     * @return
     */
    public static Set<ExpressionType> getExpressionTypes(AbstractExpression root) {
        final Set<ExpressionType> found = new HashSet<ExpressionType>();
        new ExpressionTreeWalker() {
            @Override
            protected void callback(AbstractExpression element) {
                found.add(element.getExpressionType());
            }
        }.traverse(root);
        return (found);
    }
    
    /**
     * 
     * @param <T>
     * @param root
     * @param search_class
     * @return
     */
    public static <T extends AbstractExpression> Set<T> getExpressions(AbstractExpression root, final Class<? extends T> search_class) {
        final Set<T> found = new HashSet<T>();
        new ExpressionTreeWalker() {
            @SuppressWarnings("unchecked")
            @Override
            protected void callback(AbstractExpression element) {
                if (element.getClass().equals(search_class)) {
                    found.add((T)element);
                }
                return;
            }
        }.traverse(root);
        return (found);
    }

    public static String debug(AbstractExpression exp) {
        return (ExpressionUtil.debug(exp, ""));
    }
    public static String debug(AbstractExpression exp, String spacer) {
        assert(exp != null);
        String ret = spacer + "+ " + exp.getClass().getSimpleName() + "\n";
        spacer += "   ";
        ret += spacer + "ValueType[" + exp.getValueType() + "]\n";
        
        if (exp instanceof AggregateExpression) {
            // Nothing
        } else if (exp instanceof ComparisonExpression) {
            // Nothing
        } else if (exp instanceof ConjunctionExpression) {
            // Nothing
        } else if (exp instanceof ConstantValueExpression) {
            ret += spacer + "Value[" + ((ConstantValueExpression)exp).getValue() + "]\n";
        } else if (exp instanceof InComparisonExpression) {
            InComparisonExpression in_exp = (InComparisonExpression)exp;
            ret += spacer + "Values[" + in_exp.getValues().size()+ "]:\n";
            for (int ctr = 0, cnt = in_exp.getValues().size(); ctr < cnt; ctr++) {
                ret += ExpressionUtil.debug(in_exp.getValues().get(ctr), spacer);
            } // FOR
        } else if (exp instanceof NullValueExpression) {
            // Nothing
        } else if (exp instanceof OperatorExpression) {
            // Nothing
        } else if (exp instanceof ParameterValueExpression) {
            ret += spacer + "Parameter[" + ((ParameterValueExpression)exp).getParameterId() + "]\n";
        } else if (exp instanceof TupleAddressExpression) {
            // Nothing
        } else if (exp instanceof TupleValueExpression) {
            ret += spacer + "Column Reference: ";
            ret += "[" + ((TupleValueExpression)exp).getColumnIndex() + "] ";
            ret += ((TupleValueExpression)exp).getTableName() + ".";
            ret += ((TupleValueExpression)exp).getColumnName() + " AS ";
            ret += ((TupleValueExpression)exp).getColumnAlias() + "\n";
        }
        
        //
        // Print out all of our children
        //
        if (exp.getLeft() != null || exp.getRight() != null) {
            ret += spacer + "left:  " + (exp.getLeft() != null ? "\n" + ExpressionUtil.debug(exp.getLeft(), spacer) : null + "\n");
            ret += spacer + "right:  " + (exp.getRight() != null ? "\n" + ExpressionUtil.debug(exp.getRight(), spacer) : null + "\n");
        }
        return (ret);
    }

}
