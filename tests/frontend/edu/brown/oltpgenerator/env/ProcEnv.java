package edu.brown.oltpgenerator.env;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.oltpgenerator.Utils;
import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistributionEnv;
import edu.brown.oltpgenerator.exception.TotalProbabilityExceeds100Exception;
import edu.brown.utils.ArgumentsParser;

public abstract class ProcEnv
{
    private static Comparator<Procedure>       m_compareProc      = new Comparator<Procedure>()
                                                                  {

                                                                      @Override
                                                                      public int compare(Procedure p1, Procedure p2)
                                                                      {
                                                                          return p1.getName().compareTo(p2.getName());
                                                                      }
                                                                  };

    private static Map<Procedure, MarkovGraph> s_mapProcToGraph;

    private static Map<String, Integer>        s_mapProbability   = new HashMap<String, Integer>();

    private static int                         s_nTotalProbablity = 0;

    public static void clear()
    {
        if (s_mapProcToGraph != null)
        {
            s_mapProcToGraph.clear();
        }
        s_mapProbability.clear();
        s_nTotalProbablity = 0;
    }

    public static String[] getProcNames(Procedure[] procs)
    {
        String[] ret = new String[procs.length];
        int i = 0;
        for (Procedure proc : procs)
        {
            ret[i++] = proc.getName();
        }
        return ret;
    }

    public static void loadTraceFile()
    {
        ArgumentsParser args = BenchmarkEnv.getExternalArgs();
        try
        {
            args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_MARKOV);
            Map<Procedure, MarkovGraph> tmp = null; 
//            MarkovUtil.load(args.catalog_db,; 
//                                                              args.getParam(ArgumentsParser.PARAM_MARKOV),
//                                                              CatalogUtil.getAllPartitionIds(args.catalog_db)).get(0);

            s_mapProcToGraph = filterSysProcs(tmp);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void loadDefaultParaProperty()
    {
        for (Procedure proc : s_mapProcToGraph.keySet())
        {
            for (ProcParameter para : getAllParas(proc))
            {
                RandomDistributionEnv.put(para, RandomDistributionEnv.getDefaultProperty(para));
            }
        }
    }

    public static ProcParameter[] getAllParas(Procedure proc)
    {
        ProcParameter[] ret = new ProcParameter[proc.getParameters().size()];
        Iterator<ProcParameter> iter = proc.getParameters().iterator();
        int i = 0;
        while (iter.hasNext())
        {
            ret[i++] = iter.next();
        }
        return ret;
    }

    private static Map<Procedure, MarkovGraph> filterSysProcs(Map<Procedure, MarkovGraph> map)
    {
        Map<Procedure, MarkovGraph> ret = new TreeMap<Procedure, MarkovGraph>(m_compareProc);
        for (Procedure p : map.keySet())
        {
            if (!p.getSystemproc())
            {
                ret.put(p, map.get(p));
            }
        }

        return ret;
    }

    public static Integer getProbability(String sProcName)
    {
        return s_mapProbability.get(sProcName);
    }

    public static Procedure[] getAllProcedures()
    {
        if (s_mapProcToGraph == null)
        {
            return null;
        }

        Procedure[] ret = new Procedure[s_mapProcToGraph.size()];
        int i = 0;
        for (Procedure p : s_mapProcToGraph.keySet())
        {
            ret[i++] = p;
        }

        return ret;
    }

    public static void setProbability(String curProc, int value) throws TotalProbabilityExceeds100Exception
    {
        Integer nOldVal = s_mapProbability.get(curProc);
        int nDelta = (nOldVal == null) ? value : (value - nOldVal);
        int nNewTotal = s_nTotalProbablity + nDelta;
        if (nNewTotal > 100)
        {
            throw new TotalProbabilityExceeds100Exception(nOldVal, value);
        }
        s_mapProbability.put(curProc, value);
        s_nTotalProbablity = nNewTotal;
    }

    public static Procedure getProcedure(String sProcName)
    {
        for (Procedure proc : s_mapProcToGraph.keySet())
        {
            if (proc.getName().equals(sProcName))
            {
                return proc;
            }
        }

        throw new RuntimeException("Procedure " + sProcName + " does not exist");
    }

    public static VoltType[] getParaVoltTypes(Procedure proc)
    {
        CatalogMap<ProcParameter> procParams = proc.getParameters();
        VoltType[] ret = new VoltType[procParams.size()];
        Iterator<ProcParameter> iter = procParams.iterator();
        int i = 0;
        while (iter.hasNext())
        {
            ret[i++] = VoltType.get(iter.next().getType());
        }

        return ret;
    }

    public static String[] getParaVoltTypeNames(Procedure proc)
    {
        VoltType[] types = getParaVoltTypes(proc);
        String[] ret = new String[types.length];

        for (int i = 0; i < ret.length; i++)
        {
            ret[i] = types[i].toString();
            // VoltType.xxx
            int posDot = ret[i].indexOf(".");
            ret[i] = ret[i].substring(posDot + 1);
        }

        return ret;
    }

    public static String buildVmParaList(Procedure proc)
    {
        String ret = "";

        VoltType[] voltTypes = getParaVoltTypes(proc);
        String[] javaTypes = new String[voltTypes.length];

        for (int i = 0; i < voltTypes.length; i++)
        {
            javaTypes[i] = Utils.voltTypeToJavaType(voltTypes[i]);
        }

        if (javaTypes.length > 0)
        {
            int i = 0;
            for (; i < javaTypes.length - 1; i++)
            {
                ret = ret + (javaTypes[i] + " para" + i + ", ");
            }
            ret = ret + (javaTypes[i] + " para" + i);
        }

        return ret;
    }
}
