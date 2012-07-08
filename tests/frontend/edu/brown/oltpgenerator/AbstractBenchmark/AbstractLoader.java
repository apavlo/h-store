package edu.brown.oltpgenerator.AbstractBenchmark;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.brown.api.BenchmarkComponent;
import edu.brown.oltpgenerator.RandUtil;
import edu.brown.oltpgenerator.env.TableEnv;
import edu.brown.oltpgenerator.velocity.CodeGenerator;
import edu.brown.utils.TableDataIterable;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.client.NoConnectionsException;

public abstract class AbstractLoader extends BenchmarkComponent
{
    protected AbstractLoader(String[] args)
    {
        super(args);
        // Loader class file and schema file are in the same directory
        TableEnv.setSrcSchemaPath(getSchemaFileName());
        TableEnv.readSchema();
    }

    /**
     * @return all AbstractTables to load, in topological order
     */
    protected abstract AbstractTable[] getAllTables();

    protected abstract String getSchemaFileName();

    @Override
    public String[] getTransactionDisplayNames()
    {
        return new String[] {};
    }

    @Override
    public void runLoop() throws NoConnectionsException
    {
        Map<String, VoltTable> memory = new HashMap<String, VoltTable>();
        for (AbstractTable at : getAllTables())
        {
            String sTblName = at.getName();
            VoltTable vt = TableEnv.initVoltTable(at.getName());

            String sLinkPath = at.getCsvLinkPath();
            if (sLinkPath != CodeGenerator.NO_CSV_PATH)
            {
                loadTableFromCsv(sTblName, sLinkPath, vt, memory);
            }
            else
            {
                loadTableByRandom(at, vt, memory);
            }

            System.err.println(sTblName + ": loading final " + vt.getRowCount() + " rows. ");
        }
    }

    private void loadTableFromCsv(String sTblName, String sLinkPath, VoltTable vt, Map<String, VoltTable> memory)
    {
        Table t = TableEnv.getTable(sTblName);
        try
        {
            Iterator<Object[]> iter = new TableDataIterable(t, new File(sLinkPath)).iterator();
            while (iter.hasNext())
            {
                vt.addRow(iter.next());
            }

            loadTable(sTblName, vt);
            memory.put(sTblName, vt);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private void loadTableByRandom(AbstractTable at, VoltTable vt, Map<String, VoltTable> memory)
    {
        String sTblName = at.getName();
        Column[] cols = TableEnv.getAllColumns(TableEnv.getTable(sTblName));
        Object[] row = new Object[vt.getColumnCount()];

        for (int iRow = 0; iRow < at.getCardinality(); iRow++)
        {
            for (Column col : cols)
            {
                final int iCol = col.getIndex();
                Column referredCol = TableEnv.getReferredColumn(col);
                if (referredCol != null)
                {
                    VoltTable referredTbl = memory.get(referredCol.getParent().getName());
                    row[iCol] = pickRandomVal(referredTbl, referredCol);
                }
                else
                {
                    row[iCol] = at.getColumnValGenerators()[iCol].genRandVal();
                }
            }
            vt.addRow(row);
        }
        loadTable(sTblName, vt);
        memory.put(sTblName, vt);
    }

    private Object pickRandomVal(VoltTable table, Column col)
    {
        int size = table.getRowCount();
        int i_row = RandUtil.randLong(0, size - 1).intValue();
        VoltTableRow row = table.fetchRow(i_row);
        return row.get(col.getIndex(), VoltType.get((byte) col.getType()));
    }

    protected void loadTable(String tablename, VoltTable table)
    {
        try
        {
            getClientHandle().callProcedure("@LoadMultipartitionTable", tablename, table);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
