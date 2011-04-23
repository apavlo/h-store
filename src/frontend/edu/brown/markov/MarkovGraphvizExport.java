package edu.brown.markov;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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
        boolean full_output = true;
        boolean vldb_output = false;

        Set<Procedure> procedures = new HashSet<Procedure>();
        for (String proc_name : args.getOptParam(0).split(",")) {
            if (proc_name.equals("*")) {
                for (Procedure catalog_proc : args.catalog_db.getProcedures()) {
                    if (catalog_proc.getSystemproc()) continue;
                    procedures.add(catalog_proc);
                } // FOR
            } else {
                Procedure catalog_proc = args.catalog_db.getProcedures().getIgnoreCase(proc_name);
                assert(catalog_proc != null) : "Invalid procedure '" + proc_name + "'";
                procedures.add(catalog_proc);
            }
        } // FOR
        assert(procedures.size() > 0) : "No procedures";
        
        Map<Integer, MarkovGraphsContainer> m = MarkovUtil.loadProcedures(args.catalog_db, input_path, procedures);
        for (Procedure catalog_proc : procedures) {
            MarkovGraphsContainer markovs = null;
            
            if (m.containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID)) {
                markovs = m.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
            } else {
                assert(args.getOptParamCount() > 1) : "Missing partition argument";
                int partition = args.getIntOptParam(1);
                markovs = m.get(partition);
            }
            
            Map<Integer, MarkovGraph> markov_set = markovs.getAll(catalog_proc);
            if (markov_set == null || markov_set.isEmpty()) {
                System.err.println("Skipping " + catalog_proc + " because there is no Markov graph");
                continue;
            }
            
            for (Entry<Integer, MarkovGraph> e : markov_set.entrySet()) {
                MarkovGraph markov = e.getValue();
                assert(markov.isValid()) : "The graph for " + catalog_proc + " is not initialized!";
                GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, full_output, vldb_output, null);
                
                String output = null;
                if (markov_set.size() == 1) {
                    output = gv.writeToTempFile(catalog_proc);
                } else {
                    output = gv.writeToTempFile(catalog_proc, e.getKey());
                }
                System.err.println("WROTE FILE: " + output);
            } // FOR
        } // FOR
    }
}
