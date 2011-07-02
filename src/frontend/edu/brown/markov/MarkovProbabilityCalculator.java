package edu.brown.markov;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Statement;
import org.voltdb.types.QueryType;

import edu.brown.graphs.VertexTreeWalker;
import edu.brown.markov.Vertex.Type;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class MarkovProbabilityCalculator extends VertexTreeWalker<Vertex, Edge> {
    private static final Logger LOG = Logger.getLogger(MarkovProbabilityCalculator.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final Set<Edge> visited_edges = new HashSet<Edge>();
    private final List<Integer> all_partitions;
    private MarkovEstimate markov_est;
    
    public MarkovProbabilityCalculator(MarkovGraph markov) {
        super(markov, TraverseOrder.LONGEST_PATH, Direction.REVERSE);
        
        this.all_partitions = markov.getAllPartitions();
        
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
    protected void callback(Vertex element) {
        MarkovGraph markov = (MarkovGraph)this.getGraph();
        // HACK
        Estimation est = (this.markov_est != null ? this.markov_est : element);
        
        if (trace.get()) LOG.trace("BEFORE: " + element + " => " + est.getSingleSitedProbability());
//            if (element.isSingleSitedProbablitySet() == false) element.setSingleSitedProbability(0.0);
        Type vtype = element.getType(); 
        
        // COMMIT/ABORT is always single-partitioned!
        if (vtype == Vertex.Type.COMMIT || vtype == Vertex.Type.ABORT) {
            if (trace.get()) LOG.trace(element + " is single-partitioned!");
            est.setSingleSitedProbability(1.0f);
            
            // And DONE at all partitions!
            // And will not Read/Write Probability
            for (Integer partition : this.all_partitions) {
                est.setDoneProbability(partition, 1.0f);
                est.setReadOnlyProbability(partition, 1.0f);
                est.setWriteProbability(partition, 0.0f);
            } // FOR
            
            // Abort Probability
            if (vtype == Vertex.Type.ABORT) {
                est.setAbortProbability(1.0f);
            } else {
                est.setAbortProbability(0.0f);
            }

        } else {
            
            // If the current vertex is not single-partitioned, then we know right away
            // that the probability should be zero and we don't need to check our successors
            // We define a single-partition vertex to be a query that accesses only one partition
            // that is the same partition as the base/local partition. So even if the query accesses
            // only one partition, if that partition is not the same as where the java is executing,
            // then we're going to say that it is multi-partitioned
            boolean element_islocalonly = element.isLocalPartitionOnly(); 
            if (element_islocalonly == false) {
                if (trace.get()) LOG.trace(element + " NOT is single-partitioned!");
                est.setSingleSitedProbability(0.0f);
            }

            Statement catalog_stmt = element.getCatalogItem();
            QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
            
            Collection<Edge> edges = markov.getOutEdges(element);
            for (Edge e : edges) {
                if (visited_edges.contains(e)) continue;
                Vertex successor = markov.getDest(e);
                assert(successor != null);
                assert(successor.isSingleSitedProbabilitySet()) : "Setting " + element + " BEFORE " + successor;

                // Single-Partition Probability
                // If our vertex only touches the base partition, then we need to calculate the 
                // single-partition probability as the sum of the the edge weights times our
                // successors' single-partition probability
                if (element_islocalonly) {
                    float prob = e.getProbability() * successor.getSingleSitedProbability();
                    est.addSingleSitedProbability(prob);
                    if (trace.get()) LOG.trace(element + " --" + e + "--> " + successor + String.format(" [%f * %f = %f]", e.getProbability(), successor.getSingleSitedProbability(), prob) + "\nprob = " + prob);
                }
                
                // Abort Probability
                // We need to have seen at least this number of hits before we will use a 
                // different probability that a transaction could abort
                if (element.getTotalHits() >= MarkovGraph.MIN_HITS_FOR_NO_ABORT) {
                    est.addAbortProbability(e.getProbability() * successor.getAbortProbability());
                } else {
                    est.setAbortProbability(1.0f);
                }
                
                // Done/Read/Write At Partition Probability
                for (Integer partition : all_partitions) {
                    assert(successor.isDoneProbabilitySet(partition)) : "Setting " + element + " BEFORE " + successor;
                    assert(successor.isReadOnlyProbabilitySet(partition)) : "Setting " + element + " BEFORE " + successor;
                    assert(successor.isWriteProbabilitySet(partition)) : "Setting " + element + " BEFORE " + successor;
                    
                    // This vertex accesses this partition
                    if (element.getPartitions().contains(partition)) {
                        est.setDoneProbability(partition, 0.0f);
                        
                        // Figure out whether it is a read or a write
                        if (catalog_stmt.getReadonly()) {
                            if (trace.get()) LOG.trace(String.format("%s does not modify partition %d. Setting writing probability based on children [%s]", element, partition, qtype));
                            est.addWriteProbability(partition, (e.getProbability() * successor.getWriteProbability(partition)));
                            est.addReadOnlyProbability(partition, (e.getProbability() * successor.getReadOnlyProbability(partition)));
                        } else {
                            if (trace.get()) LOG.trace(String.format("%s modifies partition %d. Setting writing probability to 1.0 [%s]", element, partition, qtype));
                            est.setWriteProbability(partition, 1.0f);
                            est.setReadOnlyProbability(partition, 0.0f);
                        }
                        
                    // This vertex doesn't access the partition, but successor vertices might so
                    // the probability is based on the edge probabilities 
                    } else {
                        est.addDoneProbability(partition, (e.getProbability() * successor.getDoneProbability(partition)));
                        est.addWriteProbability(partition, (e.getProbability() * successor.getWriteProbability(partition)));
                        est.addReadOnlyProbability(partition, (e.getProbability() * successor.getReadOnlyProbability(partition)));
                    }
                } // FOR (PartitionId)
            } // FOR (Edge)
        }
        if (trace.get()) LOG.trace("AFTER: " + element + " => " + est.getSingleSitedProbability());
        if (trace.get()) LOG.trace(StringUtil.repeat("-", 40));
    }
    
    @Override
    public void finish() {
        super.finish();
        this.visited_edges.clear();
        this.markov_est = null;
    }
    
    public static MarkovEstimate generate(MarkovGraph markov, Vertex v) {
        MarkovProbabilityCalculator calc = new MarkovProbabilityCalculator(markov);
        calc.stopAtElement(v);
        MarkovEstimate est = new MarkovEstimate(calc.all_partitions.size());
        est.init(v, MarkovEstimate.INITIAL_ESTIMATE_BATCH);
        calc.calculate(est);
//        LOG.info("MarkovEstimate:\n" + est);
//        System.exit(1);
        return (est);
    }

}
