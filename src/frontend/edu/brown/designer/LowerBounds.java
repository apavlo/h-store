package edu.brown.designer;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.partitioners.BranchAndBoundPartitioner;
import edu.brown.designer.partitioners.PartitionPlan;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.workload.AbstractWorkload;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.AbstractWorkload.Filter;

/**
 * 
 * @author pavlo
 */
public class LowerBounds {
    private static final Logger LOG = Logger.getLogger(LowerBounds.class);
    
    /**
     * InnerCostModel
     * This has to be static so that we can dynamically create it in TimeIntervalCostModel
     */
    public static class InnerCostModel extends SingleSitedCostModel {
        private static LowerBounds lb = null;
        private boolean first = true;
        
        private final TimeIntervalCostModel<SingleSitedCostModel> sscm;
        
        public InnerCostModel(Database catalog_db, PartitionEstimator p_estimator) {
            super(catalog_db, p_estimator);
            this.sscm = new TimeIntervalCostModel<SingleSitedCostModel>(catalog_db, SingleSitedCostModel.class, 1);
        }
        
        @Override
        public TransactionCacheEntry processTransaction(final Database catalog_db, final TransactionTrace txnTrace, final Filter filter) throws Exception {
            assert(lb.info != null);
            assert(lb.hints != null);
            assert(lb.designer != null);
            final boolean trace = LowerBounds.LOG.isTraceEnabled();
            final boolean debug = LowerBounds.LOG.isDebugEnabled();
            
            if (this.first) {
                this.sscm.applyDesignerHints(lb.hints);
                this.first = false;
            }
            
            final Procedure catalog_proc = txnTrace.getCatalogItem(catalog_db);
            if (trace) LowerBounds.LOG.trace("Processing " + txnTrace);
            
            // Ok so now what need to do is do a local search to find the best way to partition this mofo
            AbstractWorkload workload = new AbstractWorkload() {
                {
                    this.addTransaction(catalog_proc, txnTrace);
                }
                public void setOutputPath(String path) { }
                public void save(String path, Database catalogDb) { }
            };
            assert(workload.getTransactionCount() == 1) : "Unexpected workload size: " + workload.getTransactionCount();
            assert(workload.getTransactions().contains(txnTrace)) : "Unexpected workload contents: " + workload.getTransactions();
            if (trace) LowerBounds.LOG.trace("Created a single transaction workload");
            
            DesignerInfo info = new DesignerInfo(lb.info);
            info.setWorkload(workload);
            info.setCostModel(this.sscm);
            
            if (trace) LowerBounds.LOG.trace("Calculating optimal PartitionPlan for " + txnTrace);
            BranchAndBoundPartitioner partitioner = new BranchAndBoundPartitioner(lb.designer, info);
            partitioner.setUpperBounds(lb.hints, lb.upper_bounds, Double.MAX_VALUE, Long.MAX_VALUE);
            PartitionPlan pplan = partitioner.generate(lb.hints);
            pplan.apply(catalog_db);
            if (trace) LOG.trace("Optimal Solution for " + txnTrace + "\n" + StringUtil.box(pplan.toString()));
            this.sscm.clear();
            
            if (debug && lb.txn_ctr.incrementAndGet() % 100 == 0) {
                LOG.debug("Processed " + lb.txn_ctr.get() + " txns");
//                System.gc();
            }
            
            return (super.processTransaction(catalog_db, txnTrace, null));
        }
    } // END CLASS
    
    
    private final Designer designer;
    private final DesignerInfo info;
    private final DesignerHints hints;
    private final TimeIntervalCostModel<InnerCostModel> costmodel;
    private AtomicInteger txn_ctr = new AtomicInteger(0);
    
    private final PartitionPlan upper_bounds;
    
    /**
     * Constructor
     * @param info
     * @param hints
     * @param args
     */
    public LowerBounds(DesignerInfo info, DesignerHints hints, ArgumentsParser args) {
        // HACK!
        assert(InnerCostModel.lb == null);
        InnerCostModel.lb = this;
        
        Designer d = null;
        try {
            d = new Designer(info, hints, args);
        } catch (Exception ex) {
            LOG.fatal(ex);
            System.exit(1);
        }
        this.designer = d;
        
        this.info = info;
        this.hints = hints;
        this.costmodel = new TimeIntervalCostModel<InnerCostModel>(info.catalog_db, InnerCostModel.class, args.num_intervals);
        
        // Calculate the upper-bounds once using the default 
        this.upper_bounds = PartitionPlan.createFromCatalog(args.catalog_db);
        
        // Disable AbstractWorkload shutdown hooks
        AbstractWorkload.ENABLE_SHUTDOWN_HOOKS = false;
        
        // Disable any time limits
        this.hints.limit_back_tracks = null;
        this.hints.limit_local_time = null;
        this.hints.limit_total_time = null;
    }
    
    /**
     * 
     * @param workload
     * @return
     * @throws Exception
     */
    public double calculate(final AbstractWorkload workload) throws Exception {
        if (LOG.isDebugEnabled()) LOG.debug("Calculating lower bounds using " + workload.getTransactionCount() + " transactions" +
                                            " on " + CatalogUtil.getNumberOfPartitions(info.catalog_db) + " partitions");
        return (this.costmodel.estimateCost(this.info.catalog_db, workload));
    }
    
    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_STATS,
            ArgumentsParser.PARAM_CORRELATIONS
        );
        
        //
        // Create the container object that will hold all the information that
        // the designer will need to use
        //
        DesignerInfo info = new DesignerInfo(args);
        DesignerHints hints = args.designer_hints;
//        hints.max_memory_per_partition = Long.MAX_VALUE;
//        hints.enable_costmodel_caching = false;
        
        long start = System.currentTimeMillis();
        LowerBounds lb = new LowerBounds(info, hints, args);
        double lb_cost = lb.calculate(args.workload);
        
        LOG.info(String.format("Lower Bounds: %.03f", lb_cost));
        LOG.info("STOP: " + (System.currentTimeMillis() - start) / 1000d);
    }
}
