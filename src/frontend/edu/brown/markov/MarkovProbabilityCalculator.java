package edu.brown.markov;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Statement;
import org.voltdb.types.QueryType;

import edu.brown.graphs.VertexTreeWalker;
import edu.brown.hstore.estimators.DynamicTransactionEstimate;
import edu.brown.hstore.estimators.EstimatorUtil;
import edu.brown.hstore.estimators.markov.MarkovEstimate;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.MarkovVertex.Type;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

public class MarkovProbabilityCalculator extends VertexTreeWalker<MarkovVertex, MarkovEdge> {
    private static final Logger LOG = Logger.getLogger(MarkovProbabilityCalculator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
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
        if (trace.val) {
            String msg = String.format("START %s %s",
                                       this.getClass().getSimpleName(), this.getGraph());
            LOG.trace(StringUtil.header(msg, "=", 150));
        }
    }
    
    @Override
    protected void callback_finish() {
        super.callback_finish();
        if (trace.val) {
            String msg = String.format("STOP %s %s",
                                       this.getClass().getSimpleName(), this.getGraph());
            LOG.trace(StringUtil.header(msg, "=", 150));
        }
    }
    
    @Override
    protected void callback(MarkovVertex element) {
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        // HACK
        final DynamicTransactionEstimate est = (this.markov_est != null ? this.markov_est : element);
        final Type vtype = element.getType();
        
        if (trace.val)
            LOG.trace("BEFORE: " + element + " => " + est.getSinglePartitionProbability());
        
        // COMMIT/ABORT is always single-partitioned!
        if (vtype == MarkovVertex.Type.COMMIT || vtype == MarkovVertex.Type.ABORT) {
            est.setSinglePartitionProbability(1.0f);
            
            // And DONE at all partitions!
            // And will not Read/Write Probability
            for (Integer partition : this.all_partitions) {
                est.setDoneProbability(partition, 1.0f);
                est.setReadOnlyProbability(partition, 1.0f);
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
            // If the current vertex is not single-partitioned, then we know right away
            // that the probability should be zero and we don't need to check our successors
            // We define a single-partition vertex to be a query that accesses only one partition
            // that is the same partition as the base/local partition. So even if the query accesses
            // only one partition, if that partition is not the same as where the java is executing,
            // then we're going to say that it is multi-partitioned
//            boolean element_islocalonly = element.isLocalPartitionOnly(); 
//            if (element_islocalonly == false) {
//                if (trace.val) LOG.trace(element + " NOT is single-partitioned!");
//                est.setSinglePartitionProbability(0.0f);
//            }
            
            // Make sure everything is set to zero
            est.setSinglePartitionProbability(0f);
            for (int partition : this.all_partitions.values()) {
                est.setDoneProbability(partition, 0f);
                est.setReadOnlyProbability(partition, 0f);
                est.setWriteProbability(partition, 0f);
            } // FOR

            for (MarkovEdge e : markov.getOutEdges(element)) {
                if (this.visited_edges.contains(e)) continue;
                MarkovVertex successor = markov.getDest(e);
                assert(successor != null);
                assert(successor.isSinglePartitionProbabilitySet()) : "Setting " + element + " BEFORE " + successor;
                final Statement successorStmt = successor.getCatalogItem();
                final QueryType successorType = QueryType.get(successorStmt.getQuerytype());

                // SINGLE-PARTITION PROBABILITY
                // If our vertex only touches the base partition, then we need to calculate the 
                // single-partition probability as the sum of the the edge weights times our
                // successors' single-partition probability
//                if (element_islocalonly) {
                    float prob = e.getProbability() * successor.getSinglePartitionProbability();
                    est.addSinglePartitionProbability(prob);
                    if (trace.val) 
                        LOG.trace(String.format("%s --%s--> %s [%f * %f = %f]",
                        		  element, e, successor, 
                        		  e.getProbability(), successor.getSinglePartitionProbability(), prob));
//                }
                
                // ABORT PROBABILITY
                // We need to have seen at least this number of hits before we will use a 
                // different probability that a transaction could abort
                if (element.getTotalHits() >= MarkovGraph.MIN_HITS_FOR_NO_ABORT) {
                    est.addAbortProbability(e.getProbability() * successor.getAbortProbability());
                } else {
                    est.setAbortProbability(1.0f);
                }
                
                // DONE/READ/WRITE AT PARTITION PROBABILITY
                for (int partition : this.all_partitions.values()) {
                    assert(successor.isDoneProbabilitySet(partition)) : 
                        "Setting " + element + " BEFORE " + successor;
                    assert(successor.isReadOnlyProbabilitySet(partition)) : 
                        "Setting " + element + " BEFORE " + successor;
                    assert(successor.isWriteProbabilitySet(partition)) : 
                        "Setting " + element + " BEFORE " + successor;
                    
                    // The successor vertex accesses this partition
                    if (successor.getPartitions().contains(partition)) {
                        est.setDoneProbability(partition, 0.0f);
                        
                        // Figure out whether it is a read or a write
                        if (successorStmt.getReadonly()) {
                            if (trace.val)
                                LOG.trace(String.format("%s does not modify partition %d. " +
                                		  "Setting WRITE probability based on children [%s]",
                                          element, partition, successorType));
                            est.addWriteProbability(partition, e.getProbability()); //  * successor.getWriteProbability(partition)));
                            // est.addReadOnlyProbability(partition, 1.0f - e.getProbability()); //  * successor.getReadOnlyProbability(partition)));
                        } else {
                            if (trace.val)
                                LOG.trace(String.format("%s modifies partition %d. " +
                                		  "Setting WRITE probability to 1.0 [%s]",
                                          successor, partition, successorType));
                            // est.addWriteProbability(partition, 1.0f - e.getProbability());
                            est.addReadOnlyProbability(partition, e.getProbability());
                            // est.setWriteProbability(partition, 1.0f);
                            // est.setReadOnlyProbability(partition, 0.0f);
                        }
                    }
                    // This successor doesn't directly access this partition, so the successor's probabilities are
                    // multiplied based on the edge probabilities 
                    else {
                        float before;
                        
                        before = est.getDoneProbability(partition);
                        try {
                            est.addDoneProbability(partition, (e.getProbability() * successor.getDoneProbability(partition)));
                        } catch (Throwable ex) {
                            LOG.warn(String.format("Failed to set FINISH probability for %s [partition=%d / edge=%s / successor=%s / before=%f]",
                                                   est, partition, e, successor, before), ex);
                        }
                        before = est.getWriteProbability(partition);
                        try {
                            est.addWriteProbability(partition, (e.getProbability() * successor.getWriteProbability(partition)));
                        } catch (Throwable ex) {
                            LOG.warn(String.format("Failed to set WRITE probability for %s [partition=%d / edge=%s / successor=%s / before=%f]",
                                                   est, partition, e, successor, before), ex);
                        }
                        before = est.getReadOnlyProbability(partition);
                        try {
                            est.addReadOnlyProbability(partition, (e.getProbability() * successor.getReadOnlyProbability(partition)));
                        } catch (Throwable ex) {
                            LOG.warn(String.format("Failed to set READ-ONLY probability for %s [partition=%d / edge=%s / successor=%s / before=%f]",
                                                   est, partition, e, successor, before), ex);
                        }
                    }
                } // FOR (PartitionId)
            } // FOR (Edge)
        }
        if (trace.val) LOG.trace("AFTER: " + element + " => " + est.getSinglePartitionProbability());
        if (trace.val) LOG.trace(StringUtil.repeat("-", 40));
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
