package edu.brown.designer;

import java.io.File;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import edu.brown.catalog.CatalogUtil;
import edu.brown.costmodel.AbstractCostModel;
import edu.brown.costmodel.LowerBoundsCostModel;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.partitioners.MostPopularPartitioner;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.workload.Workload;

/**
 * 
 * @author pavlo
 */
public class LowerBoundsCalculator {
    private static final Logger LOG = Logger.getLogger(LowerBoundsCalculator.class);
    
    private final DesignerInfo info;
    private final TimeIntervalCostModel<? extends AbstractCostModel> costmodel;
    
    
    /**
     * Constructor
     * @param info
     * @param hints
     * @param args
     */
    public LowerBoundsCalculator(DesignerInfo info, int num_intervals) {
        this.info = info;
        this.costmodel = new TimeIntervalCostModel<LowerBoundsCostModel>(info.catalog_db, LowerBoundsCostModel.class, num_intervals);
    }
    
    /**
     * 
     * @param workload
     * @return
     * @throws Exception
     */
    public double calculate(final Workload workload) throws Exception {
        if (LOG.isDebugEnabled()) LOG.debug("Calculating lower bounds using " + workload.getTransactionCount() + " transactions" +
                                            " on " + CatalogUtil.getNumberOfPartitions(info.catalog_db) + " partitions");
        return (this.costmodel.estimateWorkloadCost(this.info.catalog_db, workload));
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
            ArgumentsParser.PARAM_MAPPINGS
        );
        
        // If given a PartitionPlan, then update the catalog
        if (args.hasParam(ArgumentsParser.PARAM_PARTITION_PLAN)) {
            File pplan_path = new File(args.getParam(ArgumentsParser.PARAM_PARTITION_PLAN));
            if (pplan_path.exists()) {
                PartitionPlan pplan = new PartitionPlan();
                pplan.load(pplan_path.getAbsolutePath(), args.catalog_db);
                pplan.apply(args.catalog_db);
                LOG.info("Applied PartitionPlan '" + pplan_path + "'");
            }
        }
        
        // Create the container object that will hold all the information that
        // the designer will need to use
        DesignerInfo info = new DesignerInfo(args);
        
        // Upper Bounds
        Designer designer = new Designer(info, args.designer_hints, args);
        PartitionPlan initial_solution = new MostPopularPartitioner(designer, info).generate(args.designer_hints);
        initial_solution.apply(args.catalog_db);

        // Calculate the actual cost too while we're at it...
        TimeIntervalCostModel<SingleSitedCostModel> cm = new TimeIntervalCostModel<SingleSitedCostModel>(args.catalog_db, SingleSitedCostModel.class, args.num_intervals);
        cm.applyDesignerHints(args.designer_hints);
        double upper_bound = cm.estimateWorkloadCost(args.catalog_db, args.workload);
        
        final ProfileMeasurement timer = new ProfileMeasurement("timer").start();
        LowerBoundsCalculator lb = new LowerBoundsCalculator(info, args.num_intervals);
        double lower_bound = lb.calculate(args.workload);
        timer.stop();
        
        ListOrderedMap<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("# of Partitions", CatalogUtil.getNumberOfPartitions(args.catalog_db));
        m.put("# of Intervals", args.num_intervals);
        m.put("Lower Bound", String.format("%.03f", lower_bound));
        m.put("Upper Bound", String.format("%.03f", upper_bound));
        m.put("Search Time", String.format("%.2f sec", timer.getTotalThinkTimeSeconds()));
        
        for (Entry<String, Object> e : m.entrySet()) {
            LOG.info(String.format("%-20s%s", e.getKey()+":", e.getValue().toString()));
        } // FOR
    }
}