package edu.brown.workload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;

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
		final int capacity = 35000;
		//LinkedHashMap<String, Long> LRUmap = new LinkedHashMap<String, Long>(0, (float) 0.75, true);
		final LinkedHashMap<String, Long> DiskData = new LinkedHashMap<String, Long>(0, (float) 1.0, true);
		LinkedHashMap<String, Long> LRUmap = new LinkedHashMap<String, Long>(capacity+1, 1.0f, true) {
	        protected boolean removeEldestEntry(Map.Entry<String,Long> entry)
	        {
	            if (this.size() > capacity) {
	                DiskData.put(entry.getKey(), entry.getValue());
	                return true;
	            }
	            return false;
	        };
	    };
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
							
							LRUmap.put("Name:"+col.fullName()+" Value:"+paramsArray.get(i), query.start_timestamp);
						} catch (JSONException e) {
						}											
					}
					i++;
				}

			}
		}

		//System.out.println(LRUmap);
		System.out.println(LRUmap.size());
		//System.out.println(DiskData);
		System.out.println(DiskData.size());
		Map<String, Long> result = new LinkedHashMap<String, Long>(LRUmap);
		
		result.keySet().retainAll(DiskData.keySet());
		System.out.println(result.size() + " entries were required in memory again after they were thrown");
		Iterator<Entry<String, Long>> it = result.entrySet().iterator();
		Map<Map<String, String>, Long> s = new HashMap<Map<String, String>, Long>();
		HashMap<Map<String, String>, Integer> finalResult = new HashMap<Map<String, String>, Integer>();
		while(it.hasNext()){
			Entry<String, Long> e = it.next();
			String key = e.getKey();
			int index = key.indexOf(" Value");
			String colFullName = key.substring(5, index);
			String value = key.substring(index + 8, key.length());
			Long timestamp = e.getValue();
			HashMap<String, String> map = new HashMap<String, String>();
			map.put(colFullName, value);
			int endIndex = colFullName.indexOf('.');
			String tableName = colFullName.substring(0, endIndex);
			String colName = colFullName.substring(endIndex+1, colFullName.length());
			Column column = CatalogUtil.getColumn(db, tableName, colName);
			CatalogMap<ConstraintRef> constraintMap = column.getConstraints();

			if(!constraintMap.isEmpty()){
				String foreignKey = null;
				try{
					Constraint constraint = CollectionUtil.first(constraintMap).getConstraint();
					Column foreignKeyCol = CollectionUtil.first(constraint.getForeignkeycols()).getColumn();
					foreignKey = foreignKeyCol.fullName();
				}catch (Exception exc) {
					exc.printStackTrace();
				}
				if(foreignKey!=null){
					Map<String, String> foreignKeyValueMap = new HashMap<String, String>();
					foreignKeyValueMap.put(foreignKey, value);
					if(finalResult.containsKey(foreignKeyValueMap)){
						Integer refs = finalResult.get(foreignKeyValueMap) + 1;
						finalResult.put(foreignKeyValueMap, refs);
					}else{
						finalResult.put(foreignKeyValueMap, 1);	
					}					
				}
						
			//	System.out.println("name: "+colFullName+ " value: "+ value + " time: "+ timestamp);
				//System.out.println(constraintMap);				
			}

			s.put(map, timestamp);
		}
		
		Iterator<Integer> it3 = finalResult.values().iterator();
		int overallDiskReads = 0;
		while(it3.hasNext()){
			overallDiskReads+= it3.next();
		}
		int possibleGroupings = finalResult.size();
		System.out.println("we could have grouped "+possibleGroupings + " out of "+overallDiskReads + " disk reads!!");
//		System.out.println(finalResult);
		
		return count;
	}

}
