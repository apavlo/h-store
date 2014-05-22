/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Table;
import org.voltdb.sysprocs.SysProcFragmentId;
import org.voltdb.utils.Pair;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;

import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class PartitionedTableSaveFileState extends TableSaveFileState {
    private static final Logger LOG = Logger.getLogger(PartitionedTableSaveFileState.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public PartitionedTableSaveFileState(String tableName, int allowExport) {
        super(tableName, allowExport);
    }

    @Override
    void addHostData(VoltTableRow row) throws IOException {
        assert (row.getString("TABLE").equals(getTableName()));

        if (m_totalPartitions == 0) {
            // XXX this cast should be okay unless we exceed MAX_INT partitions
            m_totalPartitions = (int) row.getLong("TOTAL_PARTITIONS");
        }
        checkSiteConsistency(row); // throws if inconsistent

        int originalPartitionId = (int) row.getLong("PARTITION");
        m_partitionsSeen.add(originalPartitionId);
        int currentHostId = (int) row.getLong("CURRENT_HOST_ID");
        Set<Pair<Integer, Integer>> partitions_at_host = null;
        if (!(m_partitionsAtHost.containsKey(currentHostId))) {
            partitions_at_host = new HashSet<Pair<Integer, Integer>>();
            m_partitionsAtHost.put(currentHostId, partitions_at_host);
        }
        partitions_at_host = m_partitionsAtHost.get(currentHostId);

        partitions_at_host.add(Pair.of(originalPartitionId, (int) row.getLong("ORIGINAL_HOST_ID")));
    }

    @Override
    public boolean isConsistent() {
        // XXX Update partition count in cluster
        return true;
        //return ((m_partitionsSeen.size() == m_totalPartitions) && (m_partitionsSeen.first() == 0) && (m_partitionsSeen.last() == m_totalPartitions - 1));
    }

    int getTotalPartitions() {
        return m_totalPartitions;
    }

    @Override
    public SynthesizedPlanFragment[] generateRestorePlan(Table catalogTable) {
        SynthesizedPlanFragment[] restore_plan = null;
        LOG.trace("Partitioned :: Total partitions for Table: " + getTableName() + ": " + getTotalPartitions());
        if (!catalogTable.getIsreplicated()) {
            restore_plan = generatePartitionedToPartitionedPlan();
        } else {
            // XXX Not implemented until we're going to support catalog changes
        }
        return restore_plan;
    }

    private void checkSiteConsistency(VoltTableRow row) throws IOException {
        if (!row.getString("IS_REPLICATED").equals("FALSE")) {
            String error = "Table: " + getTableName() + " was partitioned " + "but has a savefile which indicates replication at site: " + row.getLong("CURRENT_HOST_ID");
            throw new IOException(error);
        }

        if ((int) row.getLong("TOTAL_PARTITIONS") != getTotalPartitions()) {
            String error = "Table: " + getTableName() + " has a savefile " + " with an inconsistent number of total partitions: " + row.getLong("TOTAL_PARTITIONS") + " (previous values were "
                    + getTotalPartitions() + ") at site: " + row.getLong("CURRENT_HOST_ID");
            throw new IOException(error);
        }
    }

    private SynthesizedPlanFragment[] generatePartitionedToPartitionedPlan() {
        LOG.trace("Partition set: " + m_partitionsSeen);
        ArrayList<SynthesizedPlanFragment> restorePlan = new ArrayList<SynthesizedPlanFragment>();
        HashSet<Integer> coveredPartitions = new HashSet<Integer>();
        Iterator<Integer> hosts = m_partitionsAtHost.keySet().iterator();
        while (!coveredPartitions.containsAll(m_partitionsSeen)) {
            if (!hosts.hasNext()) {
                LOG.error("Ran out of hosts before covering all partitions with distributors");
                return null;
            }

            /**
             * Get the list of partitions on this host and remove all that were
             * covered
             */
            Integer nextHost = hosts.next();
            Set<Pair<Integer, Integer>> partitionsAndOrigHosts = new HashSet<Pair<Integer, Integer>>(m_partitionsAtHost.get(nextHost));
            Iterator<Pair<Integer, Integer>> removeCoveredIterator = partitionsAndOrigHosts.iterator();

            List<Integer> uncoveredPartitionsAtHostList = new ArrayList<Integer>();
            HashSet<Integer> originalHosts = new HashSet<Integer>();
            while (removeCoveredIterator.hasNext()) {
                Pair<Integer, Integer> p = removeCoveredIterator.next();
                if (coveredPartitions.contains(p.getFirst())) {
                    removeCoveredIterator.remove();
                } else {
                    coveredPartitions.add(p.getFirst());
                    uncoveredPartitionsAtHostList.add(p.getFirst());
                    originalHosts.add(p.getSecond());
                }
            }

            SystemProcedureExecutionContext context = this.getSystemProcedureExecutionContext();
            assert (context != null);
            Host catalog_host = context.getHost();
            Collection<Site> catalog_sites = CatalogUtil.getSitesForHost(catalog_host);

            List<Integer> sitesAtHost = new ArrayList<Integer>();
            List<Integer> partitionsAtHost = new ArrayList<Integer>();

            for (Site catalog_site : catalog_sites) {
                sitesAtHost.add(catalog_site.getId());
                for(Partition pt : catalog_site.getPartitions()){
                    partitionsAtHost.add(pt.getId());
                }
            }

            int originalHostsArray[] = new int[originalHosts.size()];
            int qq = 0;
            for (int originalHostId : originalHosts)
                originalHostsArray[qq++] = originalHostId;
            int uncoveredPartitionsAtHost[] = new int[uncoveredPartitionsAtHostList.size()];
            for (int ii = 0; ii < uncoveredPartitionsAtHostList.size(); ii++) {
                uncoveredPartitionsAtHost[ii] = uncoveredPartitionsAtHostList.get(ii);
            }

            /*
             * Assigning the FULL workload to each site. At the actual host
             * static synchronization in the procedure will ensure the work is
             * distributed across every ES in a meaningful way.
             */
            for (Integer partition : partitionsAtHost) {
                restorePlan.add(constructDistributePartitionedTableFragment(partition, uncoveredPartitionsAtHost, originalHostsArray));
            }
        }
        restorePlan.add(constructDistributePartitionedTableAggregatorFragment());
        return restorePlan.toArray(new SynthesizedPlanFragment[0]);
    }

    private SynthesizedPlanFragment constructDistributePartitionedTableFragment(int distributorPartitionId, int uncoveredPartitionsAtHost[], int originalHostsArray[]) {
        LOG.trace("constructDistributePartitionedTableFragment : to partition : " + distributorPartitionId);

        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId = SysProcFragmentId.PF_restoreDistributePartitionedTable;        
        plan_fragment.multipartition = false;
        plan_fragment.destPartitionId = distributorPartitionId;
        plan_fragment.outputDependencyIds = new int[] { result_dependency_id };
        plan_fragment.inputDependencyIds = new int[] {};
        addPlanDependencyId(result_dependency_id);
        ParameterSet params = new ParameterSet();
        params.setParameters(getTableName(), originalHostsArray, uncoveredPartitionsAtHost, result_dependency_id, m_allowExport);
        plan_fragment.parameters = params;
        return plan_fragment;
    }

    private SynthesizedPlanFragment constructDistributePartitionedTableAggregatorFragment() {
        SystemProcedureExecutionContext context = this.getSystemProcedureExecutionContext();
        assert (context != null);
        int partition_id = context.getPartitionExecutor().getPartitionId();
        LOG.trace("constructDistributePartitionedTableAggregatorFragment - partition : " + partition_id);

        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId = SysProcFragmentId.PF_restoreDistributePartitionedTableResults;
        plan_fragment.multipartition = false;
        plan_fragment.outputDependencyIds = new int[] { result_dependency_id };
        plan_fragment.inputDependencyIds = getPlanDependencyIds();
        setRootDependencyId(result_dependency_id);
        ParameterSet params = new ParameterSet();
        params.setParameters(result_dependency_id);
        plan_fragment.parameters = params;
        return plan_fragment;
    }

    // XXX-BLAH should this move to SiteTracker?
    public Set<Pair<Integer, Integer>> getPartitionsAtHost(int hostId) {
        return m_partitionsAtHost.get(hostId);
    }

    Set<Integer> getPartitionSet() {
        return m_partitionsSeen;
    }

    /**
     * Set of original PartitionId
     */
    private final TreeSet<Integer> m_partitionsSeen = new TreeSet<Integer>();

    /**
     * Map from a current host id to a pair of an original partition id and the
     * original host id
     */
    private final Map<Integer, Set<Pair<Integer, Integer>>> m_partitionsAtHost = new HashMap<Integer, Set<Pair<Integer, Integer>>>();
    private int m_totalPartitions = 0;
}
