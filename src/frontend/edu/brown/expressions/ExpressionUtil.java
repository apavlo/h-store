package edu.brown.expressions;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.InComparisonExpression;
import org.voltdb.expressions.NullValueExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleAddressExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanAssembler;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.Encoder;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * @author pavlo
 */
public abstract class ExpressionUtil extends org.voltdb.expressions.ExpressionUtil {
    private static final Logger LOG = Logger.getLogger(ExpressionUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

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
     * Recursively check whether the trees rooted at the given
     * AbstractExpressions are equal
     * 
     * @param exp0
     * @param exp1
     * @return
     */
    public static boolean equals(AbstractExpression exp0, AbstractExpression exp1) {
        if (exp0 == null) {
            return (exp1 == null);
        } else if (exp1 == null) {
            return (false);
        }
        return (exp0.equals(exp1) ? ExpressionUtil.equals(exp0.getLeft(), exp1.getLeft()) &&
                                    ExpressionUtil.equals(exp0.getRight(), exp1.getRight()) : false);
    }

    /**
     * @param catalog_db
     * @param exptree
     * @return
     * @throws Exception
     */
    public static AbstractExpression deserializeExpression(Database catalog_db, String exptree) throws Exception {
        AbstractExpression exp = null;
        if (exptree != null && !exptree.isEmpty()) {
            JSONObject json_obj = new JSONObject(Encoder.hexDecodeToString(exptree));
            exp = AbstractExpression.fromJSONObject(json_obj, catalog_db);
        }
        return (exp);
    }

    /**
     * Returns all the Column catalog handles that are access/modified in the
     * Expression tree
     * 
     * @param catalog_db
     * @param exp
     */
    public static Collection<Column> getReferencedColumns(final Database catalog_db, AbstractExpression exp) {
        final Set<Column> found_columns = new HashSet<Column>();
        new ExpressionTreeWalker() {
            @Override
            protected void callback(AbstractExpression element) {
                if (element instanceof TupleValueExpression) {
                    String table_name = ((TupleValueExpression) element).getTableName();
                    Table catalog_tbl = catalog_db.getTables().get(table_name);
                    if (catalog_tbl == null) {
                        // If it's a temp then we just ignore it. Otherwise
                        // throw an error!
                        if (table_name.contains(PlanAssembler.AGGREGATE_TEMP_TABLE) == false) {
                            this.stop();
                            throw new RuntimeException(String.format("Unknown table '%s' referenced in Expression node %s", table_name, element));
                        } else if (debug.val) {
                            LOG.debug("Ignoring temporary table '" + table_name + "'");
                        }
                        return;
                    }

                    String column_name = ((TupleValueExpression) element).getColumnName();
                    Column catalog_col = catalog_tbl.getColumns().get(column_name);
                    if (catalog_col == null) {
                        this.stop();
                        throw new RuntimeException(String.format("Unknown column '%s.%s' referenced in Expression node %s", table_name, column_name, element));
                    }
                    found_columns.add(catalog_col);
                }
                return;
            }
        }.traverse(exp);
        return (found_columns);
    }

    /**
     * Returns all the Table catalog handles that are access/modified in the
     * Expression tree
     * 
     * @param catalog_db
     * @param exp
     */
    public static Collection<Table> getReferencedTables(final Database catalog_db, AbstractExpression exp) {
        Set<Table> found_tables = new HashSet<Table>();
        for (Column catalog_col : ExpressionUtil.getReferencedColumns(catalog_db, exp)) {
            Table catalog_tbl = catalog_col.getParent();
            found_tables.add(catalog_tbl);
        } // FOR
        return (found_tables);
    }

    /**
     * Get the set of table names referenced in this expression tree. This can
     * be used without a Database catalog handle
     * 
     * @param exp
     * @return
     */
    public static Collection<String> getReferencedTableNames(AbstractExpression exp) {
        final Set<String> tableNames = new HashSet<String>();
        new ExpressionTreeWalker() {
            @Override
            protected void callback(AbstractExpression element) {
                if (element instanceof TupleValueExpression) {
                    tableNames.add(((TupleValueExpression) element).getTableName());
                }
                return;
            }
        }.traverse(exp);
        return (tableNames);
    }

    /**
     * Get all of the ExpressionTypes used in the tree below the given root
     * 
     * @param root
     * @return
     */
    public static Collection<ExpressionType> getExpressionTypes(AbstractExpression root) {
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
     * @param <T>
     * @param root
     * @param search_class
     * @return
     */
    public static <T extends AbstractExpression> Collection<T> getExpressions(AbstractExpression root, final Class<? extends T> search_class) {
        final Set<T> found = new HashSet<T>();
        new ExpressionTreeWalker() {
            @SuppressWarnings("unchecked")
            @Override
            protected void callback(AbstractExpression element) {
                if (element.getClass().equals(search_class)) {
                    found.add((T) element);
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
        assert (exp != null);
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
            sb.append(spacer).append("Value[").append(((ConstantValueExpression) exp).getValue()).append("]\n");
        } else if (exp instanceof InComparisonExpression) {
            InComparisonExpression in_exp = (InComparisonExpression) exp;
            sb.append(spacer).append("Values[").append(in_exp.getValues().size()).append("]:\n");
            for (int ctr = 0, cnt = in_exp.getValues().size(); ctr < cnt; ctr++) {
                sb.append(ExpressionUtil.debug(in_exp.getValues().get(ctr), spacer));
            } // FOR
        } else if (exp instanceof NullValueExpression) {
            // Nothing
        } else if (exp instanceof OperatorExpression) {
            name += "[" + etype.name().replace("OPERATOR_", "") + "]";
        } else if (exp instanceof ParameterValueExpression) {
            sb.append(spacer).append("Parameter[").append(((ParameterValueExpression) exp).getParameterId()).append("]\n");
        } else if (exp instanceof TupleAddressExpression) {
            // Nothing
        } else if (exp instanceof TupleValueExpression) {
            sb.append(spacer).append("Column Reference: ").append("[").append(((TupleValueExpression) exp).getColumnIndex()).append("] ").append(((TupleValueExpression) exp).getTableName())
                    .append(".").append(((TupleValueExpression) exp).getColumnName()).append(" AS ").append(((TupleValueExpression) exp).getColumnAlias()).append("\n");
        }

        // Print out all of our children
        if (exp.getLeft() != null || exp.getRight() != null) {
            sb.append(spacer).append("left:  ").append(exp.getLeft() != null ? "\n" + ExpressionUtil.debug(exp.getLeft(), spacer) : null + "\n");
            sb.append(spacer).append("right:  ").append(exp.getRight() != null ? "\n" + ExpressionUtil.debug(exp.getRight(), spacer) : null + "\n");
        }

        return (orig_spacer + "+ " + name + "\n" + sb.toString());
    }
}
