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
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Set;
import java.util.Collection;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.catalog.Table;
import org.voltdb.sysprocs.SysProcFragmentId;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;

import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class ReplicatedTableSaveFileState extends TableSaveFileState {
    private static final Logger LOG = Logger.getLogger(ReplicatedTableSaveFileState.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    ReplicatedTableSaveFileState(String tableName, int allowExport) {
        super(tableName, allowExport);
    }

    @Override
    void addHostData(VoltTableRow row) throws IOException {
        assert (row.getString("TABLE").equals(getTableName()));

        checkSiteConsistency(row); // throws if inconsistent
        // XXX this cast should be safe; site_ids are ints but get
        // promoted to long in the VoltTable.row.getLong return
        m_hostsWithThisTable.add((int) row.getLong("CURRENT_HOST_ID"));
    }

    @Override
    public boolean isConsistent() {
        // XXX right now there is nothing to check across all rows
        return true;
    }

    public Set<Integer> getHostsWithThisTable() {
        return m_hostsWithThisTable;
    }

    @Override
    public SynthesizedPlanFragment[] generateRestorePlan(Table catalogTable) {
        SystemProcedureExecutionContext context = this.getSystemProcedureExecutionContext();
        assert (context != null);
        Host catalog_host = context.getHost();
        Collection<Site> catalog_sites = CatalogUtil.getSitesForHost(catalog_host);

        LOG.info("Replicated :: Table: " + getTableName());

        Set<Integer> execution_site_ids = new TreeSet<Integer>();
        for (Site catalog_site : catalog_sites) {
            execution_site_ids.add(catalog_site.getId());
        }

        for (int hostId : m_hostsWithThisTable) {
            m_sitesWithThisTable.addAll(execution_site_ids);
        }

        SynthesizedPlanFragment[] restore_plan = null;
        if (catalogTable.getIsreplicated()) {
            restore_plan = generateReplicatedToReplicatedPlan();
        } else {
            // XXX Not implemented until we're going to support catalog changes
        }
        return restore_plan;
    }

    private void checkSiteConsistency(VoltTableRow row) throws IOException {
        if (!row.getString("IS_REPLICATED").equals("TRUE")) {
            String error = "Table: " + getTableName() + " was replicated " + "but has a savefile which indicates partitioning at site: " + row.getLong("CURRENT_HOST_ID");
            throw new IOException(error);
        }
    }

    private SynthesizedPlanFragment[] generateReplicatedToReplicatedPlan() {
        SynthesizedPlanFragment[] restore_plan = null;

        SystemProcedureExecutionContext context = this.getSystemProcedureExecutionContext();
        assert (context != null);
        Host catalog_host = context.getHost();
        Collection<Site> catalog_sites = CatalogUtil.getSitesForHost(catalog_host);

        Set<Integer> execution_site_ids = new TreeSet<Integer>();
        Set<Integer> execution_partition_ids = new TreeSet<Integer>();
        for (Site catalog_site : catalog_sites) {            
            execution_site_ids.add(catalog_site.getId());
            for(Partition pt : catalog_site.getPartitions()){
                execution_partition_ids.add(pt.getId());
            }            
        }
        
        //LOG.trace("Sites Size ::"+execution_site_ids.size());
        //LOG.trace("Partitions Size ::"+execution_partition_ids.size());

        Set<Integer> sites_missing_table = getSitesMissingTable(execution_site_ids);
        // not sure we want to deal with handling expected load failures,
        // so let's send an individual load to each site with the table
        // and then pick sites to send the table to those without it
        restore_plan = new SynthesizedPlanFragment[execution_partition_ids.size() + 1];
        int restore_plan_index = 0;

        //LOG.trace("getSitesMissingTable :");
        //for(Integer ii : sites_missing_table ) LOG.trace(" "+ii);
        
        for (Integer site_id : m_sitesWithThisTable) {
            for(Partition pt : CatalogUtil.getSiteFromId(context.getHost(),site_id).getPartitions()){
                restore_plan[restore_plan_index] = constructLoadReplicatedTableFragment();
                // XXX restore_plan[restore_plan_index].siteId = site_id;
                restore_plan[restore_plan_index].destPartitionId = pt.getId();
                ++restore_plan_index;
            }
        }
        for (Integer site_id : sites_missing_table) {
            LOG.trace("m_sites_missing_table :: site_id :"+site_id);
            int source_site_id = m_sitesWithThisTable.iterator().next(); // XXX  hacky                                                                         
            for(Partition pt : CatalogUtil.getSiteFromId(context.getHost(), source_site_id).getPartitions()){                
                restore_plan[restore_plan_index] = constructDistributeReplicatedTableFragment(source_site_id, site_id);
                ++restore_plan_index;
            }
        }
        //LOG.trace("restore_plan_index :"+restore_plan_index+" execution_partition_ids :"+execution_partition_ids.size());
        assert(restore_plan_index==execution_partition_ids.size());

        restore_plan[restore_plan_index] = constructLoadReplicatedTableAggregatorFragment();
        return restore_plan;
    }

    private Set<Integer> getSitesMissingTable(Set<Integer> clusterSiteIds) {
        Set<Integer> sites_missing_table = new HashSet<Integer>();
        for (int site_id : clusterSiteIds) {
            if (!m_sitesWithThisTable.contains(site_id)) {
                sites_missing_table.add(site_id);
            }
        }

        return sites_missing_table;
    }

    private SynthesizedPlanFragment constructLoadReplicatedTableFragment() {
        LOG.trace("constructLoadReplicatedTableFragment ");

        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId = SysProcFragmentId.PF_restoreLoadReplicatedTable;
        plan_fragment.multipartition = false;
        plan_fragment.outputDependencyIds = new int[] { result_dependency_id };
        plan_fragment.inputDependencyIds = new int[] {};
        addPlanDependencyId(result_dependency_id);
        ParameterSet params = new ParameterSet();
        params.setParameters(getTableName(), result_dependency_id, m_allowExport);
        plan_fragment.parameters = params;
        return plan_fragment;
    }

    private SynthesizedPlanFragment constructDistributeReplicatedTableFragment(int sourcePartitionId, int destinationPartitionId) {
        LOG.trace("constructDistributeReplicatedTableFragment : source -" + sourcePartitionId + " destination -" + destinationPartitionId);

        int result_dependency_id = getNextDependencyId();
        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId = SysProcFragmentId.PF_restoreDistributeReplicatedTable;
        plan_fragment.multipartition = false;
        // XXX plan_fragment.siteId = sourceSiteId;
        plan_fragment.destPartitionId = sourcePartitionId;
        plan_fragment.outputDependencyIds = new int[] { result_dependency_id };
        plan_fragment.inputDependencyIds = new int[] {};
        addPlanDependencyId(result_dependency_id);
        ParameterSet params = new ParameterSet();
        params.setParameters(getTableName(), destinationPartitionId, result_dependency_id, m_allowExport);
        plan_fragment.parameters = params;
        return plan_fragment;
    }

    private SynthesizedPlanFragment constructLoadReplicatedTableAggregatorFragment() {
        SystemProcedureExecutionContext context = this.getSystemProcedureExecutionContext();
        assert (context != null);
        int partition_id = context.getPartitionExecutor().getPartitionId();
        LOG.trace("constructLoadReplicatedTableAggregatorFragment - partition : " + partition_id);

        int result_dependency_id = getNextDependencyId();

        SynthesizedPlanFragment plan_fragment = new SynthesizedPlanFragment();
        plan_fragment.fragmentId = SysProcFragmentId.PF_restoreLoadReplicatedTableResults;
        plan_fragment.multipartition = false;
        plan_fragment.outputDependencyIds = new int[] { result_dependency_id };
        plan_fragment.inputDependencyIds = getPlanDependencyIds();
        setRootDependencyId(result_dependency_id);
        ParameterSet params = new ParameterSet();
        params.setParameters(result_dependency_id);
        plan_fragment.parameters = params;
        return plan_fragment;
    }

    private final Set<Integer> m_hostsWithThisTable = new HashSet<Integer>();
    private final Set<Integer> m_sitesWithThisTable = new HashSet<Integer>();
}
