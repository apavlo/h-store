package edu.brown.markov;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Procedure;

import edu.brown.graphs.GraphvizExport;
import edu.brown.utils.ArgumentsParser;

public class MarkovGraphvizExport {

    /**
     * @param args
     */
    public static void main(String vargs[]) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_MARKOV);
        
        String input_path = args.getParam(ArgumentsParser.PARAM_MARKOV);
        String proc_name = args.getOptParam(0);
        Procedure catalog_proc = args.catalog_db.getProcedures().getIgnoreCase(proc_name);
        assert(catalog_proc != null);
        Set<Procedure> procedures = new HashSet<Procedure>();
        procedures.add(catalog_proc);
        
        Map<Integer, MarkovGraphsContainer> m = MarkovUtil.loadProcedures(args.catalog_db, input_path, procedures);
        MarkovGraphsContainer markovs = null;
        MarkovGraph markov = null;
        if (m.containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID)) {
            markovs = m.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
            markov = markovs.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID, catalog_proc);
        } else {
            assert(args.getOptParamCount() > 1) : "Missing partition argument";
            int partition = args.getIntOptParam(1);
            markovs = m.get(partition);
            markov = markovs.get(partition, catalog_proc);
        }
        assert(markov != null);
        assert(markov.isValid()) : "The graph for " + catalog_proc + " is not initialized!";
        GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, true, null);
        System.err.println("WROTE FILE: " + gv.writeToTempFile(catalog_proc.getName()));

    }

}
