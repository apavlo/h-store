package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;

import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.interfaces.Loggable;
import edu.brown.utils.PartitionEstimator;

public class QueryPrefetcher implements Loggable {
	
	private final Database catalog_db;
	private final Map<Procedure, BatchPlanner> planners = new HashMap<Procedure, BatchPlanner>();
	
	public QueryPrefetcher(Database catalog_db, PartitionEstimator p_estimator) {
		this.catalog_db = catalog_db;
		
		// TODO: Initialize a BatchPlanner for each Procedure 
		//		 if it has the prefetch flag set to true.
		// TODO: For each Procedure, generate an array of the Statement
		//		 handles that you want to pre-fetch		
		for (Procedure proc : catalog_db.getProcedures()) {
			if (proc.getPrefetch()) {
				List<SQLStmt> prefetachable = new ArrayList<SQLStmt>();
				for (Statement catalog_stmt : proc.getStatements()) {
					if (catalog_stmt.getPrefetch()) {
						prefetachable.add(new SQLStmt(catalog_stmt));
					}
				} // FOR
				if (prefetachable.isEmpty() == false) {
					BatchPlanner planner = new BatchPlanner(prefetachable.toArray(new SQLStmt[0]),
															prefetachable.size(),
															proc,
															p_estimator);
					this.planners.put(proc, planner);
				}
			}
		} // FOR (procedure)
	}
	

	public Collection<WorkFragment> generateWorkFragments(LocalTransaction ts) {
		Procedure catalog_proc = ts.getProcedure();
		assert(ts.getProcedureParameters() != null) :
			"Unexpected null ParameterSet for " + ts;
		Object proc_params[] = ts.getProcedureParameters().toArray();
		
		// TODO: Use the StmtParameter mappings for the queries we 
		//	     want to prefetch and extract the ProcParameters 
		//		 to populate an array of ParameterSets to use as the batchArgs
		BatchPlanner planner = this.planners.get(catalog_proc);
		ParameterSet prefetch_params[] = new ParameterSet[planner.getBatchSize()];
		
		for (int i = 0; i < prefetch_params.length; i++) {
			Statement catalog_stmt = planner.getStatements()[i];
			Object stmt_params[] = new Object[catalog_stmt.getParameters().size()];
			for (StmtParameter catalog_param : catalog_stmt.getParameters()) {
				ProcParameter catalog_proc_param = catalog_param.getProcparameter();
				stmt_params[catalog_param.getIndex()] = proc_params[catalog_proc_param.getIndex()];
			} // FOR
			prefetch_params[i] = new ParameterSet(stmt_params);
		} // FOR (Prefetchable Statement)
		
		BatchPlan plan = planner.plan(ts.getTransactionId(),
									  ts.getClientHandle(),
									  ts.getBasePartition(),
									  ts.getPredictTouchedPartitions(),
									  ts.isPredictSinglePartition(),
									  ts.getTouchedPartitions(),
									  prefetch_params);
		
		return null; //plan.getWorkFragments(ts.getTransactionId(), tasks);
	}
	
	
	@Override
	public void updateLogging() {
		// TODO Auto-generated method stub

	}

}
