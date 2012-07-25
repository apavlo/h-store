package edu.brown.oltpgenerator.env;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.DependencyGraph;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.DependencyGraphGenerator;
import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistributionEnv;
import edu.brown.oltpgenerator.exception.CycleInDagException;
import edu.brown.utils.CompilerUtil;
import edu.brown.utils.FileUtil;

/**
 * Environment for table-catalog-related information
 * 
 * @author Zhe Zhang
 * 
 */
public abstract class TableEnv
{

    private static final int            DEFAULT_CARDINALITY = 1000;

    private static Map<String, Integer> s_TableCardinality  = new HashMap<String, Integer>();

    private static Catalog              s_cat               = null;

    private static String               s_sSchemaPath       = null;

    private static Map<String, String>  s_mapTableCsvLink   = new HashMap<String, String>();

    public static void clear()
    {
        s_TableCardinality.clear();
        s_cat = null;
        s_sSchemaPath = null;
        s_mapTableCsvLink.clear();
    }

    public static Catalog getCatalog()
    {
        return s_cat;
    }

    public static void setCatalog(Catalog cat)
    {
        s_cat = cat;
    }

    public static void readSchema()
    {
        try
        {
            s_cat = CompilerUtil.compileCatalog(s_sSchemaPath);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void setSrcSchemaPath(String schemaPath)
    {
        s_sSchemaPath = schemaPath;
    }

    public static void loadDefaultColumnProperty()
    {
        for (Table tbl : getAllTables())
        {
            setCardinality(tbl.getName(), DEFAULT_CARDINALITY);
            for (Column col : getAllColumns(tbl))
            {
                RandomDistributionEnv.put(col, RandomDistributionEnv.getDefaultProperty(col));
            }
        }
    }

    public static Column[] getAllColumns(Table tbl)
    {
        Column[] ret = new Column[tbl.getColumns().size()];
        Iterator<Column> iter = tbl.getColumns().iterator();
        int i = 0;
        while (iter.hasNext())
        {
            ret[i++] = iter.next();
        }
        return ret;
    }

    public static Table getTable(String tableName)
    {
        CatalogMap<Table> map = CatalogUtil.getDatabase(s_cat).getTables();
        return map.get(tableName);
    }

    public static Table[] getAllTables()
    {
        if (s_cat == null)
            throw new RuntimeException("Haven't loaded ddl");

        List<Table> tmp = new LinkedList<Table>();
        // the last element in itemsArray is null
        // so filter the array
        for (Table t : CatalogUtil.getDatabase(s_cat).getTables())
        {
            if (t != null)
                tmp.add(t);
        }
        Table[] ret = new Table[tmp.size()];
        tmp.toArray(ret);
        return ret;
    }

    public static String[] getTableNames(Table[] tables)
    {
        String[] ret = new String[tables.length];
        for (int i = 0; i < tables.length; i++)
            ret[i] = tables[i].getName();
        return ret;
    }

    public static Column getReferredColumn(Column c)
    {
        CatalogMap<ConstraintRef> crm = c.getConstraints();
        if (crm.isEmpty())
            return null;
        ConstraintRef cr = c.getConstraints().get(0);
        return cr.getConstraint().getForeignkeycols().get(0).getColumn();
    }

    public static void setCardinality(String tblName, int cardinality)
    {
        s_TableCardinality.put(tblName, cardinality);
    }

    public static Integer getCardinality(String tblName)
    {
        return s_TableCardinality.get(tblName);
    }

    /**
     * 
     * @return topologically sorted Vertices of tables in catalog
     * @throws CycleInDagException
     */
    public static Table[] sortTables() throws CycleInDagException
    {
        CatalogContext cc = new CatalogContext(s_cat.getCatalog());
        DependencyGraph dgraph = DependencyGraphGenerator.generate(cc);

        int size = dgraph.getVertexCount();

        Table[] ret = new Table[size];

        // zero_list: vertices whose in-degree is zero
        List<DesignerVertex> zero_list = new LinkedList<DesignerVertex>();
        List<DesignerVertex> non_zero_list = new LinkedList<DesignerVertex>();

        // initialize two lists
        for (DesignerVertex v : dgraph.getVertices())
        {
            if (dgraph.getPredecessorCount(v) == 0)
            {
                zero_list.add(v);
                // System.out.println("To zero_list: " + v);
            }
            else
            {
                non_zero_list.add(v);
                // System.out.println("To non_zero_list: " + v);
            }
        }

        int cnt = 0;
        while (true)
        {
            if (zero_list.isEmpty() && non_zero_list.isEmpty())
                break;

            if (zero_list.isEmpty())
            {
                throw new CycleInDagException();
            }

            DesignerVertex cur = zero_list.remove(0);
            ret[cnt++] = (Table) (cur.getCatalogItem());

            // System.out.println("Next ver: " + cur);

            Collection<DesignerVertex> successors = dgraph.getSuccessors(cur);
            dgraph.removeVertex(cur);

            for (DesignerVertex successor : successors)
            {
                if (dgraph.getPredecessorCount(successor) == 0)
                {
                    non_zero_list.remove(successor);
                    zero_list.add(successor);
                    // System.out.println("Move " + successor +
                    // " from non_zero to zero");
                }
            }
        }

        return ret;
    }

    public static boolean schemaLoaded()
    {
        return s_cat != null;
    }

    /**
     * @param tableName
     * @return an brand-new empty VoltTable whose corresponds tableName
     */
    public static VoltTable initVoltTable(String tableName)
    {
        Table t = getTable(tableName);
        VoltTable.ColumnInfo[] vtCols = new VoltTable.ColumnInfo[t.getColumns().size()];
        Iterator<Column> iCols = t.getColumns().iterator();
        while (iCols.hasNext())
        {
            Column c = iCols.next();
            vtCols[c.getIndex()] = new VoltTable.ColumnInfo(c.getName(), VoltType.get((byte) c.getType()));
        }

        return new VoltTable(vtCols);
    }

    public static String genSchemaFileName()
    {
        return BenchmarkEnv.getBenchmarkName() + "-ddl.sql";
    }

    public static String genFKSchemaFileName()
    {
        return BenchmarkEnv.getBenchmarkName() + "-ddl-fkeys.sql";
    }

    public static void genSchemaFile()
    {
        try
        {
            String content = FileUtil.readFile(s_sSchemaPath);
            FileUtil.writeStringToFile(new File(BenchmarkEnv.getProjectPath() + "/" + genSchemaFileName()), content);
            FileUtil.writeStringToFile(new File(BenchmarkEnv.getProjectPath() + "/" + genFKSchemaFileName()), content);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void linkTableToCsv(String sTblName, String sCsvPath)
    {
        s_mapTableCsvLink.put(sTblName, sCsvPath);
    }

    public static void unlinkTalbe(String sTblName)
    {
        s_mapTableCsvLink.remove(sTblName);
    }

    public static String getTableCsvLink(String sTblName)
    {
        return s_mapTableCsvLink.get(sTblName);
    }
}
