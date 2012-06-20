package edu.brown.benchmark.tpce.util;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

public abstract class ProcedureUtil {
    private static final Logger LOG = Logger.getLogger(ProcedureUtil.class.getName());

    private static final ColumnInfo[] statusColumns = { new ColumnInfo("status", VoltType.BIGINT), };

    public static VoltTable getStatusTable(boolean success) {
        VoltTable vt = new VoltTable(statusColumns);
        vt.addRow((success ? 1 : 0));
        return (vt);
    }

    /**
     * Combine multiple VoltTables into a single table
     * 
     * @param vts
     * @return
     */
    public static VoltTable combineTables(final VoltTable... vts) {
        assert (vts.length > 0);
        ColumnInfo cols[] = new ColumnInfo[vts[0].getColumnCount()];
        for (int i = 0; i < cols.length; i++) {
            cols[i] = new ColumnInfo(vts[0].getColumnName(i), vts[0].getColumnType(i));
        } // FOR

        VoltTable ret = new VoltTable(cols);
        for (VoltTable vt : vts) {
            assert (vt.getColumnCount() == ret.getColumnCount());
            vt.resetRowPosition();
            while (vt.advanceRow()) {
                ret.add(vt.cloneRow());
            } // WHILE
        } // FOR
        return (ret);
    }

    /**
     * Store
     * 
     * @param map
     * @param vt
     */
    public static void storeTableInMap(final Map<String, Object[]> map, final VoltTable vt) {
        assert (vt != null);
        assert (map != null);

        int num_rows = vt.getRowCount();
        while (vt.advanceRow()) {
            int row_idx = vt.getActiveRowIndex();
            for (int i = 0, cnt = vt.getColumnCount(); i < cnt; i++) {
                String col_name = vt.getColumnName(i);
                VoltType vtype = vt.getColumnType(i);
                if (row_idx == 0) {
                    map.put(col_name, new Object[num_rows]);
                }
                map.get(col_name)[row_idx] = vt.get(col_name, vtype);
            } // FOR
        } // WHILE
    }

    /**
     * Execute stmt with args, and put result, which is an array of columns,
     * into map
     * 
     * @param map
     *            the container of results of executing stmt
     * @param sp
     *            the store procedure that executes stmt
     * @param stmt
     *            the SQLStmt to be executed
     * @param args
     *            the argument needed by stmt
     * @param keys
     *            the names of columns in map
     * @param value_refs
     *            the references of values matching the keys. Each reference is
     *            one of {column_index, column_name, key_name}
     * @return the size of the first VoltTable among the array of VoltTable
     *         returned by executing stmt
     */
    public static int execute(Map<String, Object[]> map, VoltProcedure sp, SQLStmt stmt, Object[] args, String[] keys, Object[] value_refs) {
        LOG.info("Executing SQL: " + stmt);
System.out.println("ProcedureUtil line 97: " + args.length);
        String debug = "PARAMS:";
        for (Object arg : args) {
            debug += " " + arg;
        }
        LOG.info(debug);

        sp.voltQueueSQL(stmt, args);
        VoltTable table = sp.voltExecuteSQL()[0];
        System.out.println(table);

        if (keys == null) {
            assert (value_refs == null);
            return -1;
        }

        assert (keys.length == value_refs.length);

        int row_count = table.getRowCount();
System.out.println("ProcedureUtil line 116: " + row_count);
        // each key corresponds to a column of length row_count
        for (String key : keys)
            map.put(key, new Object[row_count]);

        // for update, delete, insert, keys is empty
        if (keys.length > 0) {
            for (int i = 0; i < row_count; i++) {
                VoltTableRow row = table.fetchRow(i);
                for (int j = 0; j < keys.length; j++) {
                    Object[] vals = map.get(keys[j]);
                    Object ref = value_refs[j];
                    if (ref instanceof Integer) {
                        // ref is column_index
                        vals[i] = row.get(j, table.getColumnType(j));
                    } else {
                        assert (ref instanceof String);
                        int idx = table.getColumnIndex((String) ref);
                        if (idx >= 0) {
                            // ref is column_name
                            vals[i] = row.get(idx, table.getColumnType(idx));
                        } else {
                            // ref is key_name
                            if (map.get(ref).length == 1) {
                                vals[i] = map.get(ref)[0];
                            } else {
                                vals[i] = map.get(ref)[i];
                            }
                        }
                    }
                }
            }
        }

        return row_count;
    }

    /**
     * Execute stmt with args
     * 
     * @param sp
     * @param stmt
     * @param args
     */
    public static void execute(VoltProcedure sp, SQLStmt stmt, Object[] args) {
        execute(null, sp, stmt, args, null, null);
    }

    /**
     * @param map
     * @return
     */
    public static VoltTable[] mapToTable(Map<String, Object[]> map) {
        VoltTable[] tables = new VoltTable[map.size()];

        int i = 0;

        Collection<String> keys = map.keySet();

        for (String key : keys) {
            Object[] vals = map.get(key);
           
            VoltTable table = new VoltTable(new VoltTable.ColumnInfo(key, ProcedureUtil.getVoltType(vals[0])));

            for (Object v : vals) {
                table.addRow(v);
            }

            tables[i++] = table;
        }

        return tables;
    }

    public static VoltType getVoltType(Object obj) {
        if (obj instanceof Byte)
            return VoltType.TINYINT;

        if (obj instanceof Short)
            return VoltType.SMALLINT;

        if (obj instanceof Integer)
            return VoltType.INTEGER;

        if (obj instanceof Long)
            return VoltType.BIGINT;

        if (obj instanceof Double || obj instanceof Float)
            return VoltType.FLOAT;

        if (obj instanceof Date)
            return VoltType.TIMESTAMP;

        if (obj instanceof String || obj instanceof Byte[])
            return VoltType.STRING;

        if (obj instanceof BigDecimal)
            return VoltType.DECIMAL;

        throw new RuntimeException("The type of " + obj + " is not supported");
    }

}
