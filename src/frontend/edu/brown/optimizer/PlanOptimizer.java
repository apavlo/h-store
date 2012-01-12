package edu.brown.optimizer;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;

import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.optimizations.AbstractOptimization;
import edu.brown.optimizer.optimizations.PushdownInlineProjectionOptimization;
import edu.brown.optimizer.optimizations.PushdownLimitOrderByOptimization;
import edu.brown.optimizer.optimizations.PushdownProjectionOptimization;
import edu.brown.optimizer.optimizations.RemoveRedundantProjectionsOptimizations;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 * @author sw47
 */
public class PlanOptimizer {
    private static final Logger LOG = Logger.getLogger(PlanOptimizer.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());

    // ----------------------------------------------------------------------------
    // GLOBAL CONFIGURATION
    // ----------------------------------------------------------------------------
    
    /**
     * The list of PlanNodeTypes that we do not want to try to optimize
     */
    private static final PlanNodeType TO_IGNORE[] = {
        PlanNodeType.AGGREGATE,
        PlanNodeType.NESTLOOP,
    };
    private static final String BROKEN_SQL[] = {
        "FROM CUSTOMER, FLIGHT, RESERVATION",   // Airline DeleteReservation.GetCustomerReservation 
        "SELECT imb_ib_id, ib_bid",             // AuctionMark NewBid.getMaxBidId
    };

    /**
     * 
     */
    protected static final Comparator<Column> COLUMN_COMPARATOR = new Comparator<Column>() {
        public int compare(Column c0, Column c1) {
            Integer i0 = c0.getIndex();
            assert (i0 != null) : "Missing index for " + c0;
            Integer i1 = c1.getIndex();
            assert (i1 != null) : "Missing index for " + c1;
            return (i0.compareTo(i1));
        }
    };
    
    /**
     * List of the Optimizations that we will want to apply
     */
    @SuppressWarnings("unchecked")
    protected static final Class<? extends AbstractOptimization> OPTIMIZATONS[] = (Class<? extends AbstractOptimization>[])new Class<?>[]{
        PushdownInlineProjectionOptimization.class,
        PushdownProjectionOptimization.class,
        PushdownLimitOrderByOptimization.class,
        RemoveRedundantProjectionsOptimizations.class,
    };
    
    // ----------------------------------------------------------------------------
    // INSTANCE CONFIGURATION
    // ----------------------------------------------------------------------------
    
    private final PlanOptimizerState state;
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------
    
    /**
     * @param context
     *            Information about context
     * @param catalogDb
     *            Catalog info about schema, metadata and procedures
     */
    public PlanOptimizer(PlannerContext context, Database catalogDb) {
        this.state = new PlanOptimizerState(catalogDb, context);
    }
    
    // ----------------------------------------------------------------------------
    // MAIN METHOD
    // ----------------------------------------------------------------------------

    /**
     * Main entry point for the PlanOptimizer
     */
    public AbstractPlanNode optimize(final String sql, final AbstractPlanNode root) {
        // HACK
        for (String broken : BROKEN_SQL) {
            if (sql.contains(broken)) {
                if (debug.get())
                    LOG.debug("Given SQL contains broken fragment '" + broken + "'. Skipping...\n" + sql);
                return (null);
            }
        }
        
        // check to see if the join nodes have the wrong offsets. If so fix them and propagate them.
        // XXX: Why is this here and not down with the rest of the stuff???
        PlanOptimizerUtil.fixJoinColumnOffsets(state, root);
        
        // Check if our tree contains anything that we want to ignore
        Collection<PlanNodeType> types = PlanNodeUtil.getPlanNodeTypes(root);
        if (trace.get())
            LOG.trace(sql + " - PlanNodeTypes: " + types);
        for (PlanNodeType t : TO_IGNORE) {
            if (types.contains(t)) {
                if (trace.get())
                    LOG.trace(String.format("Tree rooted at %s contains %s. Skipping optimization...", root, t));
                return (null);
            }
        } // FOR

        AbstractPlanNode new_root = root;
        if (debug.get())
            LOG.debug("BEFORE: " + sql + "\n" + StringUtil.box(PlanNodeUtil.debug(root)));
            
        // STEP #1:
        // Populate the PlanOptimizerState with the information that we will
        // need to figure out our various optimizations
        if (debug.get())
            LOG.debug(StringUtil.header("POPULATING OPTIMIZER STATE"));
        PlanOptimizerUtil.populateTableNodeInfo(state, new_root);
        PlanOptimizerUtil.populateJoinTableInfo(state, new_root);
            
        // STEP #2
        // Apply all the optimizations that we have
        // We will pass in the new_root each time to ensure that each optimization
        // gets a full view of the quey plan tree
        if (debug.get())
            LOG.debug(StringUtil.header("APPLYING OPTIMIZATIONS"));
        for (Class<? extends AbstractOptimization> optClass : OPTIMIZATONS) {
            if (debug.get())
                LOG.debug("Attempting to apply " + optClass.getSimpleName());
            
            // Always reset everything so that each optimization has a clean slate to work with
            state.clearDirtyNodes();
            state.updateColumnInfo(new_root);
            
            try {
                AbstractOptimization opt = ClassUtil.newInstance(optClass,
                                                                 new Object[] { state },
                                                                 new Class<?>[] { PlanOptimizerState.class });
                assert(opt != null);
                new_root = opt.optimize(new_root);
            } catch (Throwable ex) {
                LOG.error(String.format("Failed to apply %s to query plan\n%s",
                                        optClass.getSimpleName(), sql), ex);
                if (debug.get())
                    LOG.debug("Last Query Plan:\n" + PlanNodeUtil.debug(new_root));
                return (null);
            }
        } // FOR
        
        if (debug.get())
            LOG.debug("AFTER: " + sql + "\n" + StringUtil.box(PlanNodeUtil.debug(new_root)));
        
        return (new_root);
    }
    
    public PlanOptimizerState getPlanOptimizerState() {
        return this.state;
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    public static void validate(final AbstractPlanNode root) throws Exception {
        
        LOG.info("Validating: " + root + " / " + root.getPlanNodeType());
        
        switch (root.getPlanNodeType()) {
            case HASHAGGREGATE:
            case AGGREGATE: {
                // Every PlanColumn referenced in this node must appear in its children's output
                Collection<Integer> planCols = root.getOutputColumnGUIDs();
                assert(planCols != null);
                LOG.info("PLAN COLS: " + planCols);
                
                Set<Integer> foundCols = new HashSet<Integer>();
                for (AbstractPlanNode child : root.getChildren()) {
                    Collection<Integer> childCols = PlanNodeUtil.getOutputColumnIdsForPlanNode(child);
                    LOG.info("CHILD " + child + " OUTPUT: " + childCols);
                    
                    for (Integer childCol : childCols) {
                        if (planCols.contains(childCol)) {
                            foundCols.add(childCol);
                        }
                    } // FOR
                    if (foundCols.size() == planCols.size()) break;
                } // FOR
                
                if (PlanNodeUtil.getPlanNodes(root, SeqScanPlanNode.class).isEmpty() && // HACK
                    foundCols.containsAll(planCols) == false) {
                    throw new Exception(String.format("Failed to find all of the columns referenced by %s in the output columns of %s",
                                                      root, planCols));
                }
                break;
            }
            
            
        } // SWITCH
        
        for (AbstractPlanNode child : root.getChildren()) {
            PlanOptimizer.validate(child);
        }
        return;
    }

}