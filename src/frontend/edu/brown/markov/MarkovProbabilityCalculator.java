package edu.brown.markov;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Statement;

import edu.brown.graphs.VertexTreeWalker;
import edu.brown.hstore.estimators.DynamicTransactionEstimate;
import edu.brown.hstore.estimators.EstimatorUtil;
import edu.brown.hstore.estimators.markov.MarkovEstimate;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.MarkovVertex.Type;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * Utility class for computing the vertex probilities in a MarkovGraph
 * @author pavlo
 */
public class MarkovProbabilityCalculator extends VertexTreeWalker<MarkovVertex, MarkovEdge> {
    private static final Logger LOG = Logger.getLogger(MarkovProbabilityCalculator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final Set<MarkovEdge> visited_edges = new HashSet<MarkovEdge>();
    private final PartitionSet all_partitions;
    private MarkovEstimate markov_est;
    
    public MarkovProbabilityCalculator(MarkovGraph markov, PartitionSet all_partitions) {
        super(markov, TraverseOrder.LONGEST_PATH, Direction.REVERSE);
        this.all_partitions = all_partitions;
        
        // This is tricky. We need to sort of multiplex the traversal from either the commit
        // or abort vertices. We'll always start from the commit but then force the abort 
        // vertex to be the first node visited after it
        this.getChildren(markov.getCommitVertex()).addAfter(markov.getAbortVertex());
    }
    
    public void calculate() {
        this.calculate(null);
    }
        
    public void calculate(MarkovEstimate est) {
        this.markov_est = est;
        this.traverse(((MarkovGraph)this.getGraph()).getCommitVertex());
    }
    
    @Override
    protected void callback_first(MarkovVertex element) {
        super.callback_first(element);
        if (debug.val) {
            String msg = "START " + ((MarkovGraph)this.getGraph()).getProcedure().getName();
            LOG.debug(StringUtil.header(msg, "=", 150));
        }
    }
    
    @Override
    protected void callback_finish() {
        super.callback_finish();
        if (debug.val) {
            String msg = "STOP " + ((MarkovGraph)this.getGraph()).getProcedure().getName();
            LOG.debug(StringUtil.header(msg, "=", 150));
        }
    }
    
    @Override
    protected void callback(MarkovVertex element) {
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        // HACK
        final DynamicTransactionEstimate est = (this.markov_est != null ? this.markov_est : element);
        final Type vtype = element.getType();
        
        if (debug.val) {
            if (this.getCounter() > 1) LOG.debug(StringUtil.repeat("-", 100));
            LOG.debug(String.format("BEFORE: %s\n%s", element, element.debug()));
            // LOG.debug("BEFORE: " + element + " => " + est.getSinglePartitionProbability());
        }
        
        // COMMIT/ABORT is always single-partitioned!
        if (vtype == MarkovVertex.Type.COMMIT || vtype == MarkovVertex.Type.ABORT) {
//            est.setSinglePartitionProbability(1.0f);
            
            // And DONE at all partitions!
            // And will not Read/Write Probability
            for (int partition : this.all_partitions.values()) {
                est.setDoneProbability(partition, 1.0f);
//                est.setReadOnlyProbability(partition, 1.0f);
                est.setWriteProbability(partition, 0.0f);
            } // FOR
            
            // Abort Probability
            if (vtype == MarkovVertex.Type.ABORT) {
                est.setAbortProbability(1.0f);
            } else {
                est.setAbortProbability(0.0f);
            }

        }
        // STATEMENT VERTEX
        else {
            
            // Make sure everything is set to zero
//            est.setSinglePartitionProbability(0f);
            for (int partition : this.all_partitions.values()) {
                est.setDoneProbability(partition, 0f);
//                est.setReadOnlyProbability(partition, 0f);
                est.setWriteProbability(partition, 0f);
            } // FOR
            
            for (MarkovEdge e : markov.getOutEdges(element)) {
                if (this.visited_edges.contains(e)) continue;
                MarkovVertex successor = markov.getDest(e);
                assert(successor != null) :
                    "Null successor for " + e.debug(markov);
//                assert(successor.isSinglePartitionProbabilitySet()) :
//                    "Setting " + element + " BEFORE " + successor;
                assert(successor.isStartVertex() == false) :
                    "Invalid edge " + element + " --> " + successor;
                if (debug.val) 
                    LOG.debug(String.format("*** NEXT EDGE [%s --%s--> %s]",
                              element, e, successor));
                
                final Statement successorStmt = successor.getCatalogItem();
                final float edgeProbability = e.getProbability();

                // SINGLE-PARTITION PROBABILITY
                // If our successor only touches partition and that partition is also accessed
                // by the current vertex... <-- Not sure we need this last one
//                if (successor.isQueryVertex() == false || successor.getPartitions().size() == 1) { //  || element.getPartitions().containsAll(successorPartitions)) {
//                    est.addSinglePartitionProbability(edgeProbability * successor.getSinglePartitionProbability());
//                }
                
                // ABORT PROBABILITY
                // We need to have seen at least this number of hits before we will use a 
                // different probability that a transaction could abort
                if (element.getTotalHits() >= MarkovGraph.MIN_HITS_FOR_NO_ABORT) {
                    est.addAbortProbability(edgeProbability * successor.getAbortProbability());
                } else {
                    est.setAbortProbability(1.0f);
                }
                
                // DONE/READ/WRITE AT PARTITION PROBABILITY
                for (int partition : this.all_partitions.values()) {
                    boolean accessed = successor.getPartitions().contains(partition);
                    if (trace.val)
                        LOG.trace(String.format("****** PARTITION %02d [accessed=%s]",
                                  partition, accessed));
                    
                    assert(successor.isDoneProbabilitySet(partition)) : 
                        "Setting " + element + " BEFORE " + successor;
//                    assert(successor.isReadOnlyProbabilitySet(partition)) : 
//                        "Setting " + element + " BEFORE " + successor;
                    assert(successor.isWriteProbabilitySet(partition)) : 
                        "Setting " + element + " BEFORE " + successor;
                    
                    // The successor accesses this partition
                    if (accessed) {
                        // IMPORTANT: We don't want to add anything to the done probability because 
                        // it's simply based on whether the txn will go to an vertex that
                        // does not modify that partition.
                        
                        // If the query writes to this partition, then the write probability
                        // for this partition must be increased by the edge's probability
                        // This is because this is the max probability for writing to this partition
                        if (successorStmt.getReadonly() == false) {
                            if (debug.val)
                                LOG.debug(String.format("%s modifies partition %d. " +
                                          "Adding %f to WRITE probability",
                                          successor, partition, edgeProbability));
                            est.addWriteProbability(partition, edgeProbability);
                        }
                        // Otherwise, we need to set the write probability to be based on
                        // the probability that we will execute a write query at subsequent 
                        // vertices in the graph
                        else {
                            if (debug.val)
                                LOG.debug(String.format("%s does not modify partition %d. " +
                                		  "Setting WRITE probability based on children",
                                          element, partition));
                            est.addWriteProbability(partition, edgeProbability * successor.getWriteProbability(partition));
                        }
                    }
                    // This successor doesn't access this partition, so we are going to use 
                    // the successor's probabilities for our current vertex. But we have to multiply
                    // their values by the edge's probability 
                    else {
                        float before;
                        
                        // DONE
                        before = est.getDoneProbability(partition);
                        try {
                            est.addDoneProbability(partition, (edgeProbability * successor.getDoneProbability(partition)));
                        } catch (Throwable ex) {
                            LOG.warn(String.format("Failed to set FINISH probability for %s [partition=%d / edge=%s / successor=%s / before=%f]",
                                                   est, partition, e, successor, before), ex);
                        }
                        // WRITE
                        before = est.getWriteProbability(partition);
                        try {
                            est.addWriteProbability(partition, (edgeProbability * successor.getWriteProbability(partition)));
                        } catch (Throwable ex) {
                            LOG.warn(String.format("Failed to set WRITE probability for %s [partition=%d / edge=%s / successor=%s / before=%f]",
                                                   est, partition, e, successor, before), ex);
                        }
                        // READ-ONLY
//                        before = est.getReadOnlyProbability(partition);
//                        try {
//                            est.addReadOnlyProbability(partition, (edgeProbability * successor.getReadOnlyProbability(partition)));
//                        } catch (Throwable ex) {
//                            LOG.warn(String.format("Failed to set READ-ONLY probability for %s [partition=%d / edge=%s / successor=%s / before=%f]",
//                                                   est, partition, e, successor, before), ex);
//                        }
                    }
                } // FOR (PartitionId)
            } // FOR (Edge)
        }
        if (debug.val)
            LOG.debug(String.format("AFTER: %s\n%s", element, element.debug()));
    }
    
    @Override
    public void finish() {
        super.finish();
        this.visited_edges.clear();
        this.markov_est = null;
    }
    
    public static MarkovEstimate generate(CatalogContext catalogContext, MarkovGraph markov, MarkovVertex v) {
        MarkovProbabilityCalculator calc = new MarkovProbabilityCalculator(markov, catalogContext.getAllPartitionIds());
        calc.stopAtElement(v);
        MarkovEstimate est = new MarkovEstimate(catalogContext);
        est.init(v, EstimatorUtil.INITIAL_ESTIMATE_BATCH);
        calc.calculate(est);
//        LOG.info("MarkovEstimate:\n" + est);
//        System.exit(1);
        return (est);
    }

}
