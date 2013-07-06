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

package org.voltdb.compiler;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.hsqldb.HSQLInterface;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.ParameterInfo;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.DeletePlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.QueryType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.Encoder;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.PlanFragmentIdGenerator;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;

/**
 * Compiles individual SQL statements and updates the given catalog.
 * <br/>Invokes the Optimizer to generate plans.
 *
 */
public abstract class StatementCompiler {
    private static final Logger LOG = Logger.getLogger(StatementCompiler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();

    private static AtomicInteger NEXT_FRAGMENT_ID = null;
    
    public static int getNextFragmentId(Database catalog_db) {
        return getNextFragmentId(catalog_db, false, false, false);
    }
    
    public synchronized static int getNextFragmentId(Database catalog_db,
                                                        boolean readonly,
                                                        boolean fastAggregate,
                                                        boolean fastCombine) {
        // If this is the first time we are being called, figure out
        // where our ids should start at
        if (NEXT_FRAGMENT_ID == null) {
            int max_id = 100;
            for (Statement catalog_stmt : CatalogUtil.getAllStatements(catalog_db)) {
                for (PlanFragment catalog_frag : CatalogUtil.getAllPlanFragments(catalog_stmt)) {
                    max_id = Math.max(max_id, catalog_frag.getId());
                } // FOR
            } // FOR
            NEXT_FRAGMENT_ID = new AtomicInteger(max_id);
            if (trace.val) LOG.trace("Initialized NEXT_FRAGMENT_ID = " + NEXT_FRAGMENT_ID.get());
        }
        // If it's not readonly, then we'll offset it so that we can
        // easily identify it at runtime
        int next_id = NEXT_FRAGMENT_ID.incrementAndGet();
        return (PlanFragmentIdGenerator.createPlanFragmentId(next_id,
                                                              readonly,
                                                              fastAggregate,
                                                              fastCombine));
    }
    
    public static void compile(VoltCompiler compiler, HSQLInterface hsql,
            Catalog catalog, Database db, DatabaseEstimates estimates,
            Statement catalogStmt, String stmt, boolean singlePartition)
    throws VoltCompiler.VoltCompilerException {

        // Always add in a unique Id
        catalogStmt.setId(compiler.getNextStatementId());
        
        // Strip newlines for catalog compatibility
        stmt = stmt.replaceAll("\n", " ");
        // remove leading and trailing whitespace so the lines not
        // too far below this doesn't fail (starts with "insert", etc...)
        stmt = stmt.trim();

        //LOG.fine("Compiling Statement: ");
        //LOG.fine(stmt);
        compiler.addInfo("Compiling Statement: " + stmt);

        // determine the type of the query
        QueryType qtype;
        if (stmt.toLowerCase().startsWith("insert")) {
            qtype = QueryType.INSERT;
            catalogStmt.setReadonly(false);
        }
        else if (stmt.toLowerCase().startsWith("update")) {
            qtype = QueryType.UPDATE;
            catalogStmt.setReadonly(false);
        }
        else if (stmt.toLowerCase().startsWith("delete")) {
            qtype = QueryType.DELETE;
            catalogStmt.setReadonly(false);
        }
        else if (stmt.toLowerCase().startsWith("select")) {
            qtype = QueryType.SELECT;
            catalogStmt.setReadonly(true);
        }
        else {
            throw compiler.new VoltCompilerException("Unparsable SQL statement.");
        }
        catalogStmt.setQuerytype(qtype.getValue());

        // put the data in the catalog that we have
        catalogStmt.setSqltext(stmt);
        catalogStmt.setSinglepartition(singlePartition);
        catalogStmt.setBatched(false);
        catalogStmt.setParamnum(0);
        catalogStmt.setHas_singlesited(false);
        catalogStmt.setHas_multisited(false);

        // PAVLO: Super Hack!
        // Always compile the multi-partition and single-partition query plans!
        // We don't need the multi-partition query plans for MapReduce transactions
        Procedure catalog_proc = catalogStmt.getParent();
        boolean isMapReduce = catalog_proc.getMapreduce();

        CompiledPlan plan = null;
        CompiledPlan last_plan = null;
        PlanNodeList node_list = null;
        
        QueryPlanner planner = new QueryPlanner(catalog.getClusters().get("cluster"), db, hsql, estimates, true, false);

        Throwable first_exception = null;
        for (boolean _singleSited : new boolean[]{ true, false }) {
            if (_singleSited == false && isMapReduce) continue;
            
            QueryType stmt_type = QueryType.get(catalogStmt.getQuerytype());
            String msg = "Creating " + stmt_type.name() + " query plan for " + catalogStmt.fullName() + ": singleSited=" + _singleSited;
            if (trace.val) LOG.trace(msg);
            compiler.addInfo(msg);

            catalogStmt.setSinglepartition(_singleSited);
            String name = catalogStmt.getParent().getName() + "-" + catalogStmt.getName();
    
            TrivialCostModel costModel = new TrivialCostModel();
            try {
                plan = planner.compilePlan(costModel, catalogStmt.getSqltext(),
                        catalogStmt.getName(), catalogStmt.getParent().getName(),
                        catalogStmt.getSinglepartition(), null);
            } catch (Throwable e) {
                LOG.error("Failed to plan for stmt: " + catalogStmt.fullName(), e);
                if (first_exception == null) {
                    if (debug.val) LOG.warn("Ignoring first error for " + catalogStmt.getName() + " :: " + e.getMessage());
                    first_exception = e;
                    continue;
                }
                e.printStackTrace();
                throw compiler.new VoltCompilerException("Failed to plan for stmt: " + catalogStmt.fullName());
            }

            if (plan == null) {
                msg = "Failed to plan for stmt: " + catalogStmt.fullName();
                String plannerMsg = planner.getErrorMessage();

                if (plannerMsg == null) plannerMsg = "PlannerMessage was empty!";
                
                // HACK: Ignore if they were trying to do a single-sited INSERT/UPDATE/DELETE
                //       on a replicated table
                if (plannerMsg.contains("replicated table") && _singleSited) {
                    if (debug.val) 
                        LOG.warn(String.format("Ignoring error for %s: %s", catalogStmt.fullName(), plannerMsg));
                    continue;
                // HACK: If we get an unknown error message on an multi-sited INSERT/UPDATE/DELETE, assume
                //       that it's because we are trying to insert on a non-replicated table
                } else if (!_singleSited && stmt_type == QueryType.INSERT && plannerMsg.contains("Error unknown")) {
                    if (debug.val) 
                        LOG.warn(String.format("Ignoring multi-sited %s %s on non-replicated table: %s",
                                               stmt_type.name(), catalogStmt.fullName(), plannerMsg));
                    continue;
                } else if (planner.getError() != null) {
                    if (debug.val) LOG.error(msg);
                    throw compiler.new VoltCompilerException(msg, planner.getError());
                // Otherwise, report the error
                } else {
                    if (plannerMsg != null)
                        msg += " with error: \"" + plannerMsg + "\"";
                    if (debug.val) LOG.error(msg);
                    throw compiler.new VoltCompilerException(msg);
                }
            }
            if (trace.val)
                LOG.trace(String.format("%s Analyzing %s query plan",
                                        catalogStmt.fullName(), (_singleSited == false ? "DTXN" : "SP")));  

            // serialize full where clause to the catalog
            // for the benefit of the designer
            if (plan.fullWhereClause != null) {
                String json = "ERROR";
                try {
                    // serialize to pretty printed json
                    String jsonCompact = plan.fullWhereClause.toJSONString();
                    // pretty printing seems to cause issues
                    //JSONObject jobj = new JSONObject(jsonCompact);
                    //json = jobj.toString(4);
                    json = jsonCompact;
                } catch (Exception e) {
                    // hopefully someone will notice
                    e.printStackTrace();
                }
                String hexString = Encoder.hexEncode(json);
                catalogStmt.setExptree(hexString);
            }

            // serialize full plan to the catalog
            // for the benefit of the designer
            if (plan.fullWinnerPlan != null) {
                String json = plan.fullplan_json;
                String hexString = Encoder.hexEncode(json);
                if (_singleSited) {
                    catalogStmt.setFullplan(hexString);
                } else {
                    catalogStmt.setMs_fullplan(hexString);
                }
            }
    
            //Store the list of parameters types and indexes in the plan node list.
    
            /*List<Pair<Integer, VoltType>> parameters = node_list.getParameters();
            for (ParameterInfo param : plan.parameters) {
                Pair<Integer, VoltType> parameter = new Pair<Integer, VoltType>(param.index, param.type);
                parameters.add(parameter);
            }*/
    
            int i = 0;
            Collections.sort(plan.fragments);
            if (trace.val)
                LOG.trace(catalogStmt.fullName() + " Plan Fragments: " + plan.fragments);
            for (CompiledPlan.Fragment fragment : plan.fragments) {
                node_list = new PlanNodeList(fragment.planGraph);
                
                boolean readonly = fragmentReadOnly(fragment.planGraph);
                boolean fastAggregate = false; // FIXME
                boolean fastCombine = false; // FIXME
                
                // Now update our catalog information
                int id = getNextFragmentId(db, readonly, fastAggregate, fastCombine);
                String planFragmentName = Integer.toString(id);
                PlanFragment planFragment = null;
                    
                if (_singleSited) {
                    planFragment = catalogStmt.getFragments().add(planFragmentName);
                    catalogStmt.setHas_singlesited(true);
                    if (trace.val)
                        LOG.trace(String.format("%s SP PLAN FRAGMENT: %s", catalogStmt.fullName(), planFragment));
                } else {
                    planFragment = catalogStmt.getMs_fragments().add(planFragmentName);
                    catalogStmt.setHas_multisited(true);
                    if (trace.val)
                        LOG.trace(String.format("%s DTXN PLAN FRAGMENT: %s", catalogStmt.fullName(), planFragment));
                }
    
                // mark a fragment as non-transactional if it never touches a persistent table
                planFragment.setNontransactional(!fragmentReferencesPersistentTable(fragment.planGraph));
                planFragment.setReadonly(readonly);
                planFragment.setHasdependencies(fragment.hasDependencies);
                planFragment.setMultipartition(fragment.multiPartition);
                planFragment.setId(id);

                String json = null;
                try {
                    JSONObject jobj = new JSONObject(node_list.toJSONString());
                    json = jobj.toString(4);
                } catch (JSONException e2) {
                    throw new RuntimeException(e2);
                }
    
                // TODO: can't re-enable this until the EE accepts PlanColumn GUIDs
                // instead of column names because the deserialization is done without
                // any connection to the child nodes - required to map the PlanColumn's
                // GUID to the child's column name.
    
                // verify the plan serializes and deserializes correctly.
                // assert(node_list.testJSONSerialization(db));
    
                // output the plan to disk for debugging
                PrintStream plansOut = BuildDirectoryUtils.getDebugOutputPrintStream(
                        "statement-winner-plans", name + "-" + String.valueOf(i++) + ".txt");
                plansOut.println(json);
                plansOut.close();
    
                //
                // We then stick a serialized version of PlanNodeTree into a PlanFragment
                //
                try {
                    FastSerializer fs = new FastSerializer(false, false); // C++ needs little-endian
                    fs.write(json.getBytes());
                    String hexString = fs.getHexEncodedBytes();
                    planFragment.setPlannodetree(hexString);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw compiler.new VoltCompilerException(e.getMessage());
                }
            }
            
            last_plan = plan;
        } // FOR (multipartition + singlepartition)
        if (last_plan == null) {
            throw compiler.new VoltCompilerException("Bad news! We don't have a last plan!!");
        }
        plan = last_plan;
        
        // HACK
        AbstractPlanNode root = null;
        try {
            root = PlanNodeUtil.getRootPlanNodeForStatement(catalogStmt, true);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        assert(root != null);
        Collection<Table> tables_accessed = CatalogUtil.getReferencedTablesForTree(db, root);
        assert(tables_accessed.isEmpty() == false) : "Failed to find accessed tables for " + catalogStmt + "-- Plan:\n" + PlanNodeUtil.debug(plan.fullWinnerPlan);
        boolean all_replicated = true;
        for (Table catalog_tbl : tables_accessed) {
            if (catalog_tbl.getIsreplicated() == false) {
                all_replicated = false;
                break;
            }
        } // FOR
        catalogStmt.setReplicatedonly(all_replicated);
        
        // Input Parameters
        // We will need to update the system catalogs with this new information
        // If this is an ad hoc query then there won't be any parameters
        for (ParameterInfo param : plan.parameters) {
            StmtParameter catalogParam = catalogStmt.getParameters().add(String.valueOf(param.index));
            catalogParam.setJavatype(param.type.getValue());
            catalogParam.setIndex(param.index);
        }

        // Output Columns
        int index = 0;
        for (Integer colguid : plan.columns) {
            PlanColumn planColumn = planner.getPlannerContext().get(colguid);
            Column catColumn = catalogStmt.getOutput_columns().add(planColumn.getDisplayName()); // String.valueOf(index));
            catColumn.setNullable(false);
            catColumn.setIndex(index);
//            catColumn.setName(planColumn.displayName());
            catColumn.setType(planColumn.type().getValue());
            catColumn.setSize(planColumn.width());
            index++;
        }

        catalogStmt.setReplicatedtabledml(plan.replicatedTableDML);

        //Store the list of parameters types and indexes in the plan node list.

        /*List<Pair<Integer, VoltType>> parameters = node_list.getParameters();
        for (ParameterInfo param : plan.parameters) {
            Pair<Integer, VoltType> parameter = new Pair<Integer, VoltType>(param.index, param.type);
            parameters.add(parameter);
        }*/


    }

    /**
     * Check through a plan graph and return true if it ever touches a persistent table.
     */
    static boolean fragmentReferencesPersistentTable(AbstractPlanNode node) {
        if (node == null)
            return false;

        // these nodes can read/modify persistent tables
        if (node instanceof AbstractScanPlanNode)
            return true;
        if (node instanceof InsertPlanNode)
            return true;
        if (node instanceof DeletePlanNode)
            return true;
        if (node instanceof UpdatePlanNode)
            return true;

        // recursively check out children
        for (int i = 0; i < node.getChildPlanNodeCount(); i++) {
            AbstractPlanNode child = node.getChild(i);
            if (fragmentReferencesPersistentTable(child))
                return true;
        }

        // if nothing found, return false
        return false;
    }
    
    /**
     * Check through a plan graph and return true if it is read only
     */
    static boolean fragmentReadOnly(AbstractPlanNode node) {
        if (node == null)
            return true;

        if (node instanceof InsertPlanNode)
            return false;
        if (node instanceof DeletePlanNode)
            return false;
        if (node instanceof UpdatePlanNode)
            return false;

        // recursively check out children
        for (int i = 0; i < node.getChildPlanNodeCount(); i++) {
            if (fragmentReadOnly(node.getChild(i)) == false) return (false);
        }

        // if nothing found, return true
        return true;
    }
}
