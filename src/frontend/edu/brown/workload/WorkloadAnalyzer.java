package edu.brown.workload;

import java.util.ArrayList;
import java.util.Collection;


import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.types.ExpressionType;

import edu.brown.catalog.CatalogPair;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.PredicatePairs;

public class WorkloadAnalyzer {

	private Workload workload;
	private Database db;

	public WorkloadAnalyzer(Database catalog_db, Workload workload) {
		this.db = catalog_db;
		this.workload = workload;
	}

	/**
	 * @param args
	 */

	public static void main(String[] vargs) throws Exception {
		ArgumentsParser args = ArgumentsParser.load(vargs);
		args.require(ArgumentsParser.PARAM_CATALOG,
				ArgumentsParser.PARAM_WORKLOAD);

		WorkloadAnalyzer analyzer = new WorkloadAnalyzer(args.catalog_db,
				args.workload);
		int count = analyzer.getCountOfReferencesInInterval(20000);
		System.out.println("count is " + count);

	}

	public int getCountOfReferencesInInterval(int timeInterval) {
		/*simulate the anticache 
		for each txn in workload
			for each query
				what tables referenced, what keys
				put those keys in a queue
				the queue is an LRU thing
				once queue becomes full, it means that now something will be thrown out to disk
				count the number of times something is got from disk

		repeat with correlation of tables in mind
		 */
		int count = 0;
		String sqlText1 = null, sqlText2 = null;
		Collection<Table> table1 = null, table2 = null;
		String procName = "InsertCallForwarding";
		LinkedHashMap<String, Boolean> LRUmap = new LinkedHashMap<String, Boolean>(0, (float) 0.75, true);
		for (TransactionTrace txn : workload) {
			for (QueryTrace query : txn.getQueries()) {
				
				Statement stmt = query.getCatalogItem(db);
				
				if(stmt.fullName().equals("GetNewDestination.GetData"))
					continue;
				
				Collection<Table> tables = CatalogUtil.getReferencedTables(stmt);
				List<Column> allPrimaryKeyColumns = new ArrayList<Column>();
				for(Table table : tables){
					Collection<Column> cols = CatalogUtil.getPrimaryKeyColumns(table);
					allPrimaryKeyColumns.addAll(cols);					
				}
				
				JSONObject json_object = null;
				try {
					json_object = new JSONObject(query.toJSONString(db));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				JSONArray paramsArray = null;
				try {
					paramsArray = json_object.getJSONArray(("PARAMS"));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Collection<Column> columnsReferenced = CatalogUtil.getReferencedColumns(stmt);
				//System.out.println(columnsReferenced.size() + " " +paramsArray.length());
				int i = 0;
				for(Column col : columnsReferenced){
					if(allPrimaryKeyColumns.contains(col)){
						try {
							//System.out.println("about to put in"+ col);
							LRUmap.put(col.fullName()+paramsArray.get(i), true);
						} catch (JSONException e) {
						}											
					}
					i++;
				}

//				System.out.println(query.getParams());
//				System.out.println(stmt.getSqltext());
				CatalogMap<StmtParameter> params = stmt.getParameters();
				Iterator<StmtParameter> it = params.iterator();
//				System.out.println(query.getParams());
//				while(it.hasNext()){
//					StmtParameter stmtParam = it.next();
//					System.out.println(stmtParam.fullName());
//				}
				//System.out.println(allPrimaryKeyColumns);
//				PredicatePairs predicates = CatalogUtil
//						.extractStatementPredicates(stmt, true);
//				if(paramsArray.length()!=predicates.size())
//					System.out.println(stmt.fullName());
//					//System.out.println(stmt.getSqltext()+" "+paramsArray.length()+" "+predicates.size());
//				for(CatalogPair predicate :predicates){
//					String p1 = predicate.getFirst().fullName();
//					//System.out.println("first param" + params.get(p1));
//					String p2 = predicate.getSecond().fullName();
//					if(paramsArray.length()!=predicates.size())
//						System.out.println(p2);
//					//System.out.println("second param" + params.get(p2));
//					int index = predicates.indexOf(predicate);
//					//System.out.println(p1);
//					if(allPrimaryKeyColumns.contains(p1))
//						try {
//							System.out.println("about to put in"+ p1);
//							LRUmap.put(p1, p2+paramsArray.get(index));
//						} catch (JSONException e) {
//							// TODO Auto-generated catch block
//							//e.printStackTrace();
//						}
////					if(allPrimaryKeyColumns.contains(p2))
////						LRUmap.put(p2, true);
//					
//				}
				//Collection<Table> tables = CatalogUtil.getReferencedTables(stmt);
//				Collection<Column> columns = CatalogUtil.getReferencedColumns(stmt);
//				System.out.println(columns);
//				System.out.println(tables);
//				CatalogUtil.getForeignKeyDependents(tables.get(0));
//				CatalogUtil.getModifiedColumns(stmt);
//				CatalogUtil.getReadOnlyColumns(stmt);
				//CatalogUtil.getStmtParameters(stmt, catalog_proc_param)
				
//				Procedure proc = query.getCatalogProcedure(db);
//
//				if (proc.getName().equals(procName)) {
//
//					Statement stmt = query.getCatalogItem(db);
//					if (stmt.getName().equals("query1")) {
//						sqlText1 = stmt.getSqltext();
//						table1 = CatalogUtil.getReferencedTables(stmt);
//						outputColumnToRemember = stmt.getOutput_columns()
//								.get(0);
//						rememberedTimestamp = query.getStartTimestamp();
//					}
//					if (stmt.getName().equals("query2")) {
//						sqlText2 = stmt.getSqltext();
//						table2 = CatalogUtil.getReferencedTables(stmt);
//						PredicatePairs predicates = CatalogUtil
//								.extractStatementPredicates(stmt, true);
//						if (outputColumnToRemember != null) {
//							Long difference = query.getStartTimestamp()
//									- rememberedTimestamp;
//							// System.out.println(query.getStartTimestamp() -
//							// rememberedTimestamp);
//							boolean predicateKeyEqualsPrevOutputColumn = predicates
//									.get(0).getSecond().getName()
//									.equals(outputColumnToRemember.getName());
//							boolean isEqualToComparison = predicates.get(0)
//									.getComparisonExp()
//									.equals(ExpressionType.COMPARE_EQUAL);
//							if (predicateKeyEqualsPrevOutputColumn
//									&& isEqualToComparison
//									&& difference < timeInterval)
//								count++;
//							outputColumnToRemember = null;
//							rememberedTimestamp = null;
//						}
//					}
//
//					/*
//					 * SELECT S_ID FROM SUBSCRIBER WHERE SUB_NBR = ?
//					 * 
//					 * and SELECT SF_TYPE FROM SPECIAL_FACILITY WHERE S_ID = ?
//					 */
//				}
//
			}
		}

		System.out.println(LRUmap);
		System.out.println(LRUmap.size());
//		System.out.println("*************************");
//		System.out.println("Analyzing the following queries in procedure "
//				+ procName);
//		System.out.println(sqlText1);
//		System.out.println(sqlText2);
//		System.out.println("Referenced record: row from table " + table2);
//		System.out.println("In conjunction with: row from table " + table1);
//		System.out.println("Time interval of window: " + timeInterval);
//		System.out.println("*************************");
		return count;
	}

}
