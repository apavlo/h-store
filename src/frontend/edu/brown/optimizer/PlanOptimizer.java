package edu.brown.optimizer;

import java.util.Collection;
import java.util.Comparator;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.PlanNodeType;
import org.voltdb.utils.Pair;

import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.optimizations.AbstractOptimization;
import edu.brown.optimizer.optimizations.AggregatePushdownOptimization;
import edu.brown.optimizer.optimizations.CombineOptimization;
import edu.brown.optimizer.optimizations.LimitPushdownOptimization;
import edu.brown.optimizer.optimizations.ProjectionPushdownOptimization;
import edu.brown.optimizer.optimizations.RemoveDistributedReplicatedTableJoinOptimization;
import edu.brown.optimizer.optimizations.RemoveRedundantProjectionsOptimizations;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.StringBoxUtil;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 * @author sw47
 */
public class PlanOptimizer {
    private static final Logger LOG = Logger.getLogger(PlanOptimizer.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();

    // ----------------------------------------------------------------------------
    // GLOBAL CONFIGURATION
    // ----------------------------------------------------------------------------

    /**
     * The list of PlanNodeTypes that we do not want to try to optimize
     */
    private static final PlanNodeType TO_IGNORE[] = { PlanNodeType.AGGREGATE, PlanNodeType.NESTLOOP, };
    private static final String BROKEN_SQL[] = {
            // "FROM CUSTOMER, FLIGHT, RESERVATION", // Airline DeleteReservation.GetCustomerReservation
            // "SELECT imb_ib_id, ib_bid", // AuctionMark NewBid.getMaxBidId
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
    protected static final Class<? extends AbstractOptimization> OPTIMIZATONS[] = 
        (Class<? extends AbstractOptimization>[]) new Class<?>[] {
            RemoveDistributedReplicatedTableJoinOptimization.class,    
            AggregatePushdownOptimization.class,
            ProjectionPushdownOptimization.class,
            LimitPushdownOptimization.class,
            RemoveRedundantProjectionsOptimizations.class,
            CombineOptimization.class,
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
        try {
            return _optimize(sql, root);
        } catch (Throwable ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
        
    public AbstractPlanNode _optimize(final String sql, final AbstractPlanNode root) {
        // HACK
        for (String broken : BROKEN_SQL) {
            if (sql.contains(broken)) {
                if (debug.val)
                    LOG.debug("Given SQL contains broken fragment '" + broken + "'. Skipping...\n" + sql);
                return (null);
            }
        }

        // check to see if the join nodes have the wrong offsets. If so fix them
        // and propagate them.
        // XXX: Why is this here and not down with the rest of the stuff???
        PlanOptimizerUtil.fixJoinColumnOffsets(state, root);

        // Check if our tree contains anything that we want to ignore
        Collection<PlanNodeType> types = PlanNodeUtil.getPlanNodeTypes(root);
        if (trace.val)
            LOG.trace(sql + " - PlanNodeTypes: " + types);
        for (PlanNodeType t : TO_IGNORE) {
            if (types.contains(t)) {
                if (trace.val)
                    LOG.trace(String.format("Tree rooted at %s contains %s. Skipping optimization...", root, t));
                return (null);
            }
        } // FOR

        // Skip single partition query plans
        // if (types.contains(PlanNodeType.RECEIVE) == false) return (null);

        AbstractPlanNode new_root = root;
        if (trace.val)
            LOG.trace("BEFORE: " + sql + "\n" + StringBoxUtil.box(PlanNodeUtil.debug(root)));
//             LOG.debug("LET 'ER RIP!");
//         }

        // STEP #1:
        // Populate the PlanOptimizerState with the information that we will
        // need to figure out our various optimizations
        if (debug.val)
            LOG.debug(StringUtil.header("POPULATING OPTIMIZER STATE", "*"));
        PlanOptimizerUtil.populateTableNodeInfo(state, new_root);
        PlanOptimizerUtil.populateJoinTableInfo(state, new_root);

        // STEP #2
        // Apply all the optimizations that we have
        // We will pass in the new_root each time to ensure that each
        // optimization
        // gets a full view of the quey plan tree
        if (debug.val)
            LOG.debug(StringUtil.header("APPLYING OPTIMIZATIONS", "*"));
        for (Class<? extends AbstractOptimization> optClass : OPTIMIZATONS) {
            if (debug.val)
                LOG.debug(StringUtil.header(optClass.getSimpleName()));

            // Always reset everything so that each optimization has a clean
            // slate to work with
            state.clearDirtyNodes();
            state.updateColumnInfo(new_root);

            try {
                AbstractOptimization opt = ClassUtil.newInstance(optClass,
                                                                 new Object[] { state },
                                                                 new Class<?>[] { PlanOptimizerState.class });
                assert (opt != null);
                Pair<Boolean, AbstractPlanNode> p = opt.optimize(new_root);
                if (p.getFirst()) {
                    if (debug.val)
                        LOG.debug(String.format("%s modified query plan", optClass.getSimpleName()));
                    new_root = p.getSecond();
                }
            } catch (Throwable ex) {
                if (debug.val)
                    LOG.debug("Last Query Plan:\n" + PlanNodeUtil.debug(new_root));

                String msg = String.format("Failed to apply %s to query plan\n%s", optClass.getSimpleName(), sql);
                if (ex instanceof AssertionError)
                    throw new RuntimeException(msg, ex);
                LOG.warn(msg, ex);
                return (null);
            }

            // STEP #3
            // If any nodes were modified by this optimization, go through the tree
            // and make sure our output columns and other information is all in sync
            if (state.hasDirtyNodes())
                PlanOptimizerUtil.updateAllColumns(state, new_root, false);
        } // FOR
        PlanOptimizerUtil.updateAllColumns(state, new_root, true);

        if (trace.val)
            LOG.trace("AFTER: " + sql + "\n" + StringBoxUtil.box(PlanNodeUtil.debug(new_root)));

        return (new_root);
    }

    public PlanOptimizerState getPlanOptimizerState() {
        return this.state;
    }

}