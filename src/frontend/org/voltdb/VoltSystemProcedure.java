/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;

/**
 * System procedures extend VoltSystemProcedure and use its utility methods to
 * create work in the system. This functionality is not available to standard
 * user procedures (which extend VoltProcedure).
 */
public abstract class VoltSystemProcedure extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(VoltSystemProcedure.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /** Standard column type for host/partition/site id columns */
    protected static VoltType CTYPE_ID = VoltType.INTEGER;

    /** Standard column name for a host id column */
    protected static String CNAME_HOST_ID = "HOST_ID";

    /** Standard column name for a site id column */
    protected static String CNAME_SITE_ID = "SITE_ID";

    /** Standard column name for a partition id column */
    protected static String CNAME_PARTITION_ID = "PARTITION_ID";

    /** Standard schema for sysprocs returning a simple status table */
    public static ColumnInfo STATUS_SCHEMA =
        new ColumnInfo("STATUS", VoltType.BIGINT);   // public to fix javadoc linking warning

    /** Standard success return value for sysprocs returning STATUS_SCHEMA */
    protected static long STATUS_OK = 0L;
    
    protected CatalogContext catalogContext;
    
    protected final List<WorkFragment.Builder> fragments = new ArrayList<WorkFragment.Builder>();

    public abstract void initImpl();
    
    @Override
    public final void init(PartitionExecutor executor,
                           Procedure catalog_proc,
                           BackendTarget eeType) {
        super.init(executor, catalog_proc, eeType);
        this.catalogContext = executor.getCatalogContext();
        this.initImpl();
    }

    protected final void registerPlanFragment(long fragId) {
        this.executor.registerPlanFragment(fragId, this);
    }
    
//    protected final AbstractTransaction getTransactionState(Long txnId) {
//        return (this.executor.getHStoreSite().getTransaction(txnId));
//    }
    
    /** Bundles the data needed to describe a plan fragment. */
    public static class SynthesizedPlanFragment {
        public int destPartitionId = -1;
        public int fragmentId = -1;
        public int inputDependencyIds[] = null;
        public int outputDependencyIds[] = null;
        public ParameterSet parameters = null;
        public boolean multipartition = false;   /** true if distributes to all executable partitions */
        public boolean nonExecSites = false;     /** true if distributes once to each node */
        public boolean last_task = false;
    }

    abstract public DependencySet executePlanFragment(Long txn_id,
                                                      Map<Integer,List<VoltTable>> dependencies,
                                                      int fragmentId,
                                                      ParameterSet params,
                                                      PartitionExecutor.SystemProcedureExecutionContext context);

    /**
     * Produce work units, possibly on all sites, for a list of plan fragments.
     * The final plan fragment must aggregate intermediate results and produce
     * a single output dependency. This aggregate output is returned as the result.
     *
     * @param pfs an array of synthesized plan fragments
     * @param aggregatorOutputDependencyId dependency id produced by the aggregation pf
     *        The id of the table returned as the result of this procedure.
     * @return the resulting VoltTable as a length-one array.
     */
    protected VoltTable[] executeSysProcPlanFragments(SynthesizedPlanFragment pfs[],
                                                      int aggregatorOutputDependencyId) {
        // Block until we get all of our responses.
        // We can do this because our ExecutionSite is multi-threaded
        return (executeSysProcPlanFragmentsAsync(pfs));
    }

    /**
     * Produce work units, possibly on all sites, for a list of plan fragments.
     * The final plan fragment must aggregate intermediate results and produce
     * a single output dependency. This aggregate output is returned as the result.
     *
     * @param pfs an array of synthesized plan fragments
     * @param aggregatorOutputDependencyId dependency id produced by the aggregation pf
     *        The id of the table returned as the result of this procedure.
     */
    protected final VoltTable[] executeSysProcPlanFragmentsAsync(SynthesizedPlanFragment pfs[]) {
        LocalTransaction ts = this.getTransactionState();
        if (debug.val) LOG.debug(ts + " - Preparing to execute " + pfs.length + " sysproc fragments");
        
        this.fragments.clear();
        ParameterSet parameters[] = new ParameterSet[pfs.length];
        for (int i = 0; i < pfs.length; i++) {
            SynthesizedPlanFragment pf = pfs[i];
            // check mutually exclusive flags
            assert(!(pf.multipartition && pf.nonExecSites));
            assert(pf.parameters != null);

            // We'll let the PartitionExecutor decide how to serialize our ParameterSets
            parameters[i] = pf.parameters;

            // If the multipartition flag is set to true and we don't have a destPartitionId,
            // then we'll just make it go to all partitions. This is so that we can support
            // old-school VoltDB's sysprocs
            int partitions[] = null;
            if (pf.destPartitionId < 0) {
                if (pf.multipartition) {
                    partitions = CollectionUtil.toIntArray(catalogContext.getAllPartitionIds());
                }
                // If it's not multipartitioned and they still don't have a destPartitionId,
                // then we'll make it just go to this PartitionExecutor's local partition
                else {
                    partitions = new int[]{ this.executor.getPartitionId() };
                }
                if (debug.val) LOG.debug(this.getClass() + " => " + Arrays.toString(partitions));
            }
            else {
                partitions = new int[]{ pf.destPartitionId };
            }
            
            // Create a WorkFragment for each target partition
            for (int destPartitionId : partitions) {
                if (debug.val) 
                    LOG.debug(String.format("%s - Creating %s for partition %s [fragmentId=%d]",
                              ts, WorkFragment.class.getSimpleName(),
                              destPartitionId, pf.fragmentId));
                WorkFragment.Builder builder = WorkFragment.newBuilder()
                                                        .setPartitionId(destPartitionId)
                                                        .setReadOnly(false)
                                                        .setLastFragment(pf.last_task)
                                                        .addFragmentId(pf.fragmentId)
                                                        .addStmtCounter(0)
                                                        .addStmtIndex(0)
                                                        .addStmtIgnore(false)
                                                        .addParamIndex(i);
                ts.getTouchedPartitions().put(destPartitionId);
                
                boolean needs_input = false;
                for (int ii = 0; ii < pf.outputDependencyIds.length; ii++) {
                    // Input Dependencies
                    if (pf.inputDependencyIds != null && ii < pf.inputDependencyIds.length) {
                        builder.addInputDepId(pf.inputDependencyIds[ii]);
                        needs_input = needs_input || (pf.inputDependencyIds[ii] != HStoreConstants.NULL_DEPENDENCY_ID);
                    } else {
                        builder.addInputDepId(HStoreConstants.NULL_DEPENDENCY_ID);
                    }
                    // Output Dependencies
                    builder.addOutputDepId(pf.outputDependencyIds[ii]);
                } // FOR
                builder.setNeedsInput(needs_input);
                this.fragments.add(builder);
            } // FOR
        } // FOR

        // For some reason we have problems if we're using the transaction profiler
        // with sysprocs, so we'll just always turn it off
        if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.disableProfiling();
        
        // Bombs away!
        return (this.executor.dispatchWorkFragments(ts, 1, parameters, this.fragments, false));
    }
    
    /**
     * Helper method that will return true if the invoking partition
     * is the first partition at this HStoreSite
     * @return
     */
    protected final boolean isFirstLocalPartition() {
        return (Collections.min(hstore_site.getLocalPartitionIds()) == this.partitionId);
    }
    
    /**
     * Helper method that will queue and execute the given SynthesizedPlanFragment id 
     * at the local partition
     * @param fragId
     * @param params
     * @return
     */
    protected final VoltTable[] executeLocal(final int fragId, final ParameterSet params) {
        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
        
        int i = 0;
        pfs[i] = new SynthesizedPlanFragment();
        pfs[i].fragmentId = fragId;
        pfs[i].inputDependencyIds = new int[] { };
        pfs[i].outputDependencyIds = new int[] { fragId };
        pfs[i].multipartition = true;
        pfs[i].nonExecSites = false;
        pfs[i].destPartitionId = this.partitionId;
        pfs[i].parameters = params;
        pfs[i].last_task = true;
        
        return (this.executeSysProcPlanFragments(pfs, fragId));
    }
    
    protected final VoltTable[] executeOncePerSite(final int distributeId, final int aggregateId) {
        return this.executeOncePerSite(distributeId, aggregateId, new ParameterSet());
    }
    
    /**
     * Helper method that will queue up the distributed SynthesizedPlanFragment (distributeId)
     * at the first partition at each site and then execute the aggregate 
     * SynthesizedPlanFragment (aggregateId) at the local partition. Note that the same
     * ParameterSet will be given to both the distributed and aggregate operations
     * @param distributeId
     * @param aggregateId
     * @param params
     * @return
     */
    protected final VoltTable[] executeOncePerSite(final int distributeId, final int aggregateId, final ParameterSet params) {
        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[catalogContext.numberOfSites + 1];
        
        int i = 0;
        for (Site catalog_site : catalogContext.sites.values()) {
            Partition catalog_part = null;
            int first_id = Integer.MAX_VALUE;
            for (Partition p : catalog_site.getPartitions().values()) {
                if (catalog_part == null || p.getId() < first_id) {
                    catalog_part = p;
                    first_id = p.getId();
                }
            } // FOR
            assert(catalog_part != null) : "No partitions for " + catalog_site;

            if (debug.val)
                LOG.debug(String.format("Creating PlanFragment #%d for %s on %s",
                          distributeId, catalog_part, catalog_site));
            pfs[i] = new SynthesizedPlanFragment();
            pfs[i].fragmentId = distributeId;
            pfs[i].inputDependencyIds = new int[] { };
            pfs[i].outputDependencyIds = new int[] { distributeId };
            pfs[i].multipartition = true;
            pfs[i].nonExecSites = false;
            pfs[i].destPartitionId = catalog_part.getId();
            pfs[i].parameters = params;
            pfs[i].last_task = (catalog_site.getId() != hstore_site.getSiteId());
            i += 1;
        } // FOR

        // a final plan fragment to aggregate the results
        pfs[i] = new SynthesizedPlanFragment();
        pfs[i].fragmentId = aggregateId;
        pfs[i].inputDependencyIds = new int[] { distributeId };
        pfs[i].outputDependencyIds = new int[] { aggregateId };
        pfs[i].multipartition = false;
        pfs[i].nonExecSites = false;
        pfs[i].destPartitionId = this.partitionId;
        pfs[i].parameters = params;
        pfs[i].last_task = true;
        
        return (this.executeSysProcPlanFragments(pfs, aggregateId));
    }
    
    /**
     * Helper method that will queue up the distributed SynthesizedPlanFragment (distributeId)
     * at all of the partitions in the cluster. Note that the same ParameterSet will be given 
     * to both the distributed and aggregate operations
     * @param distributeId
     * @param aggregateId
     * @param params
     * @return
     */
    protected final VoltTable[] executeOncePerPartition(final int distributeId, final int aggregateId, final ParameterSet params) {
        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[catalogContext.numberOfPartitions + 1];
        
        int i = 0;
        for (Partition catalog_part : catalogContext.getAllPartitions()) {
            pfs[i] = new SynthesizedPlanFragment();
            pfs[i].fragmentId = distributeId;
            pfs[i].inputDependencyIds = new int[] { };
            pfs[i].outputDependencyIds = new int[] { distributeId };
            pfs[i].multipartition = true;
            pfs[i].nonExecSites = false;
            pfs[i].destPartitionId = catalog_part.getId();
            pfs[i].parameters = params;
            pfs[i].last_task = false; // (catalog_part.getId() != this.partitionId);
            i += 1;
        } // FOR

        // a final plan fragment to aggregate the results
        pfs[i] = new SynthesizedPlanFragment();
        pfs[i].fragmentId = aggregateId;
        pfs[i].inputDependencyIds = new int[] { distributeId };
        pfs[i].outputDependencyIds = new int[] { aggregateId };
        pfs[i].multipartition = false;
        pfs[i].nonExecSites = false;
        pfs[i].destPartitionId = this.partitionId;
        pfs[i].parameters = params;
        pfs[i].last_task = true;
        
        return (this.executeSysProcPlanFragments(pfs, aggregateId));
    }
    
    /**
     * Returns the formatted procedure name to use to invoke the given sysproc class
     * This is what is passed into the client
     * @param procClass
     * @return
     */
    public static final String procCallName(Class<? extends VoltSystemProcedure> procClass) {
        return "@" + procClass.getSimpleName();
    }
}
