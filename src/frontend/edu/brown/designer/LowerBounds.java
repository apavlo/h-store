package edu.brown.designer;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.partitioners.BranchAndBoundPartitioner;
import edu.brown.designer.partitioners.PartitionPlan;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
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
     */
    public static class InnerCostModel extends SingleSitedCostModel {
        private static LowerBounds lb = null;
        
        private final SingleSitedCostModel sscm;
        
        public InnerCostModel(Database catalog_db, PartitionEstimator p_estimator) {
            super(catalog_db, p_estimator);
            this.sscm = new SingleSitedCostModel(catalog_db, p_estimator);
        }
        
        
        @Override
        public TransactionCacheEntry processTransaction(final Database catalog_db, final TransactionTrace txnTrace, final Filter filter) throws Exception {
            assert(lb.info != null);
            assert(lb.hints != null);
            assert(lb.designer != null);
            
            final Procedure catalog_proc = txnTrace.getCatalogItem(catalog_db);
            
            // Ok so now what need to do is do a local search to find the best way to partition this mofo
            AbstractWorkload workload = new AbstractWorkload() {
                {
                    this.addTransaction(catalog_proc, txnTrace);
                }
                public void setOutputPath(String path) { }
                public void save(String path, Database catalogDb) { }
            };
            
            DesignerInfo info = new DesignerInfo(lb.info);
            info.setWorkload(workload);
            info.setCostModel(this.sscm);
            BranchAndBoundPartitioner partitioner = new BranchAndBoundPartitioner(lb.designer, info);
            PartitionPlan pplan = partitioner.generate(lb.hints);
            pplan.apply(catalog_db);
            
            return super.processTransaction(catalog_db, txnTrace, filter);
        }
    } // END CLASS
    
    
    private final Designer designer;
    private final DesignerInfo info;
    private final DesignerHints hints;
    private final TimeIntervalCostModel<InnerCostModel> costmodel;
    
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
    }
    
    /**
     * 
     * @param workload
     * @return
     * @throws Exception
     */
    public double calculate(final AbstractWorkload workload) throws Exception {
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
        
        long start = System.currentTimeMillis();
        LowerBounds lb = new LowerBounds(info, hints, args);
        double lb_cost = lb.calculate(args.workload);
        
        LOG.info(String.format("Lower Bounds: %.03f", lb_cost));
        LOG.info("STOP: " + (System.currentTimeMillis() - start) / 1000d);
    }
}
