package edu.brown.workload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ArgumentsParser;

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
					e.printStackTrace();
				}
				JSONArray paramsArray = null;
				try {
					paramsArray = json_object.getJSONArray(("PARAMS"));
				} catch (JSONException e) {
					e.printStackTrace();
				}
				Collection<Column> columnsReferenced = CatalogUtil.getReferencedColumns(stmt);
				int i = 0;
				for(Column col : columnsReferenced){
					if(allPrimaryKeyColumns.contains(col)){
						try {
							LRUmap.put(col.fullName()+paramsArray.get(i), true);
						} catch (JSONException e) {
						}											
					}
					i++;
				}

			}
		}

		System.out.println(LRUmap);
		System.out.println(LRUmap.size());
		return count;
	}

}
