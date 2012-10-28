package edu.brown.hstore.estimators;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Statement;

import edu.brown.graphs.GraphvizExport;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.estimators.markov.MarkovEstimate;
import edu.brown.hstore.estimators.markov.MarkovEstimatorState;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.MarkovVertex;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.StringUtil;

public abstract class EstimatorUtil {
    private static final Logger LOG = Logger.getLogger(EstimatorUtil.class);

    /**
     * This marker is used to indicate that an Estimate is for the 
     * initial evaluation of a txn (i.e., before it has started running 
     * and submitted any query batches). 
     */
    public static final int INITIAL_ESTIMATE_BATCH = -1;
    
    /**
     * The value to use to indicate that a probability is null
     */
    public static final float NULL_MARKER = -1.0f;

    public static String mispredictDebug(LocalTransaction ts,
                                         BatchPlanner planner,
                                         SQLStmt batchStmts[],
                                         ParameterSet params[]) {
        StringBuilder sb = new StringBuilder();
        Exception ex = ts.getPendingError();
        sb.append("Caught " + ex.getClass().getSimpleName() + "!\n")
          .append(StringUtil.SINGLE_LINE);

        // TRANSACTION STATE
        sb.append("\nTRANSACTION STATE\n").append(ts.debug());
        
        // BATCH PLAN
        sb.append("CURRENT BATCH\n");
        for (int i = 0; i < planner.getBatchSize(); i++) {
            sb.append(String.format("[%02d] %s <==> %s\n     %s\n     %s\n",
                         i,
                         batchStmts[i].getStatement().fullName(),
                         planner.getStatements()[i].fullName(),
                         batchStmts[i].getStatement().getSqltext(),
                         Arrays.toString(params[i].toArray())));
        } // FOR
        
        // BATCH PLANNER
        sb.append("\nPLANNER\n");
        for (int i = 0; i < planner.getBatchSize(); i++) {
            Statement stmt0 = planner.getStatements()[i];
            Statement stmt1 = batchStmts[i].getStatement();
            assert(stmt0.fullName().equals(stmt1.fullName())) : 
                stmt0.fullName() + " != " + stmt1.fullName(); 
            sb.append(String.format("[%02d] %s\n     %s\n", i, stmt0.fullName(), stmt1.fullName()));
        } // FOR
        
        // PARAMETERS
        sb.append("\nBATCH PARAMETERS\n");
        ParameterMangler pm = ParameterMangler.singleton(ts.getProcedure());
        Object mangled[] = pm.convert(ts.getProcedureParameters().toArray());
        sb.append(pm.toString(mangled));
        
        // ESTIMATOR STATE
        sb.append("\nESTIMATOR STATE:\n");
        EstimatorState s = ts.getEstimatorState();
        if (s instanceof MarkovEstimatorState) {
            MarkovGraph markov = ((MarkovEstimatorState)s).getMarkovGraph();
            MarkovEstimate initialEst = s.getInitialEstimate();
            List<MarkovEdge> initialPath = markov.getPath(initialEst.getMarkovPath());
            List<MarkovVertex> actualPath = ((MarkovEstimatorState)s).getActualPath();
            
            sb.append(s.toString());
            try {
                GraphvizExport<MarkovVertex, MarkovEdge> gv = MarkovUtil.exportGraphviz(markov, true, initialPath);
                gv.highlightPath(markov.getPath(actualPath), "blue");
                LOG.info("PARTITION: " + ts.getBasePartition());
                LOG.info("GRAPH: " + gv.writeToTempFile(ts.getProcedure().getName()));
            } catch (Exception ex2) {
                LOG.fatal("???????????????????????", ex2);
            }
        } else {
            sb.append("No TransactionEstimator.State! Can't dump out MarkovGraph!\n");
        }
        
        return (sb.toString());
    }
    
}
