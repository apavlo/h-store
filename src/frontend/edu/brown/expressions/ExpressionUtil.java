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
        final String orig_spacer = spacer;
        String name = exp.getClass().getSimpleName();
        ExpressionType etype = exp.getExpressionType();
        
        final StringBuilder sb = new StringBuilder();
        spacer += "   ";
        sb.append(spacer).append("ValueType[").append(exp.getValueType()).append("]\n");
        
        if (exp instanceof AggregateExpression) {
            // Nothing
        } else if (exp instanceof ComparisonExpression) {
            name += "[" + etype.name().replace("COMPARE_", "") + "]";
        } else if (exp instanceof ConjunctionExpression) {
            name += "[" + etype.name().replace("CONJUNCTION_", "") + "]";
        } else if (exp instanceof ConstantValueExpression) {
            sb.append(spacer).append("Value[").append(((ConstantValueExpression)exp).getValue()).append("]\n");
        } else if (exp instanceof InComparisonExpression) {
            InComparisonExpression in_exp = (InComparisonExpression)exp;
            sb.append(spacer).append("Values[").append(in_exp.getValues().size()).append("]:\n");
            for (int ctr = 0, cnt = in_exp.getValues().size(); ctr < cnt; ctr++) {
                sb.append(ExpressionUtil.debug(in_exp.getValues().get(ctr), spacer));
            } // FOR
        } else if (exp instanceof NullValueExpression) {
            // Nothing
        } else if (exp instanceof OperatorExpression) {
            name += "[" + etype.name().replace("OPERATOR_", "") + "]";
        } else if (exp instanceof ParameterValueExpression) {
            sb.append(spacer).append("Parameter[").append(((ParameterValueExpression)exp).getParameterId()).append("]\n");
        } else if (exp instanceof TupleAddressExpression) {
            // Nothing
        } else if (exp instanceof TupleValueExpression) {
        	//System.out.println((((TupleValueExpression)exp).getColumnName() + " column index: " + (((TupleValueExpression)exp).getColumnIndex())));
            sb.append(spacer).append("Column Reference: ")
              .append("[").append(((TupleValueExpression)exp).getColumnIndex()).append("] ")
              .append(((TupleValueExpression)exp).getTableName()).append(".")
              .append(((TupleValueExpression)exp).getColumnName()).append(" AS ")
              .append(((TupleValueExpression)exp).getColumnAlias()).append("\n");
        }
        
        // Print out all of our children
        if (exp.getLeft() != null || exp.getRight() != null) {
            sb.append(spacer).append("left:  ").append(exp.getLeft() != null ? "\n" + ExpressionUtil.debug(exp.getLeft(), spacer) : null + "\n");
            sb.append(spacer).append("right:  ").append(exp.getRight() != null ? "\n" + ExpressionUtil.debug(exp.getRight(), spacer) : null + "\n");
        }
        
        return (orig_spacer + "+ " + name  + "\n" + sb.toString()); 
    }

}
