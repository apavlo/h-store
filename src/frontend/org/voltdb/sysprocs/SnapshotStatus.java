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

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;

@ProcInfo(singlePartition = false)
public class SnapshotStatus extends VoltSystemProcedure {

    @Override
    public void initImpl() {
        // Nothing
    }

    @Override
    public DependencySet executePlanFragment(Long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, final SystemProcedureExecutionContext context) {
        return null;
    }

    public VoltTable[] run() throws VoltAbortException {
        ArrayList<Integer> catalogIds = new ArrayList<Integer>();
        catalogIds.add(0);
        return new VoltTable[] { VoltDB.instance().getStatsAgent().getStats(SysProcSelector.SNAPSHOTSTATUS, catalogIds, false, System.currentTimeMillis()) };
    }
}
