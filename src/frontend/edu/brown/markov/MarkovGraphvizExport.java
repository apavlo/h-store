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
        
        Set<Procedure> procedures = new HashSet<Procedure>();
        for (String proc_name : args.getOptParam(0).split(",")) {
            if (proc_name.equals("*")) {
                for (Procedure catalog_proc : args.catalog_db.getProcedures()) {
                    if (catalog_proc.getSystemproc()) continue;
                    procedures.add(catalog_proc);
                } // FOR
            } else {
                Procedure catalog_proc = args.catalog_db.getProcedures().getIgnoreCase(proc_name);
                assert(catalog_proc != null);
                procedures.add(catalog_proc);
            }
        } // FOR
        assert(procedures.size() > 0) : "No procedures";
        
        Map<Integer, MarkovGraphsContainer> m = MarkovUtil.loadProcedures(args.catalog_db, input_path, procedures);
        for (Procedure catalog_proc : procedures) {
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
            if (markov == null) {
                System.err.println("Skipping " + catalog_proc + " because there is no Markov graph");
                continue;
            }
            assert(markov.isValid()) : "The graph for " + catalog_proc + " is not initialized!";
            
            boolean full_output = false;
            boolean vldb_output = true;
            GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, full_output, vldb_output, null);
            System.err.println("WROTE FILE: " + gv.writeToTempFile(catalog_proc.getName()));
        } // FOR
    }

}
