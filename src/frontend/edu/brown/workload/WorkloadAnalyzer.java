package edu.brown.workload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
		analyzer.getCountOfGroupingsPossible();
	}

	public int getCountOfGroupingsPossible() {
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
		final int mainMemMapCapacity = 300000;
		final LinkedHashMap<String, Long> DiskData = new LinkedHashMap<String, Long>(0, (float) 1.0, true);
		LinkedHashMap<String, Long> memoryLRUMap = new LinkedHashMap<String, Long>(mainMemMapCapacity+1, 1.0f, true) {
	        protected boolean removeEldestEntry(Map.Entry<String,Long> entry)
	        {
	            if (this.size() > mainMemMapCapacity) {
	                DiskData.put(entry.getKey(), entry.getValue());
	                return true;
	            }
	            return false;
	        };
	    };
	    Long duration = workload.getMaxStartTimestamp() - workload.getMinStartTimestamp();
		for (TransactionTrace txn : workload) {
			for (QueryTrace query : txn.getQueries()) {
				
				Statement stmt = query.getCatalogItem(db);
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
				int paramIndex = 0;
				for(Column col : columnsReferenced){
					if(allPrimaryKeyColumns.contains(col)){
						try {
							
							memoryLRUMap.put("Name:"+col.fullName()+" Value:"+paramsArray.get(paramIndex), query.start_timestamp);
						} catch (JSONException e) {
						}											
					}
					paramIndex++;
				}

			}
		}

		System.out.println("Main mem size is " + memoryLRUMap.size());
		System.out.println("Disk data (anticache analogous) size is " + DiskData.size());
		Map<String, Long> result = new LinkedHashMap<String, Long>(memoryLRUMap);
		
		result.keySet().retainAll(DiskData.keySet());
		System.out.println(result.size() + " entries were required in memory again after they were thrown");
		Iterator<Entry<String, Long>> it = result.entrySet().iterator();
		HashMap<Map<String, String>, List<Long>> finalResult = new HashMap<Map<String, String>, List<Long>>();
		while(it.hasNext()){
			Entry<String, Long> entry = it.next();
			String key = entry.getKey();
			int index = key.indexOf(" Value");
			String colFullName = key.substring(5, index);
			String value = key.substring(index + 8, key.length());
			Long timestamp = entry.getValue();
			HashMap<String, String> map = new HashMap<String, String>();
			map.put(colFullName, value);
			int endIndex = colFullName.indexOf('.');
			String tableName = colFullName.substring(0, endIndex);
			String colName = colFullName.substring(endIndex+1, colFullName.length());
			Column column = CatalogUtil.getColumn(db, tableName, colName);
			CatalogMap<ConstraintRef> constraintMap = column.getConstraints();

			if(!constraintMap.isEmpty()){
				String foreignKey = null;
				Constraint constraint = CollectionUtil.first(constraintMap).getConstraint();
				Column foreignKeyCol = CollectionUtil.first(constraint.getForeignkeycols()).getColumn();
				foreignKey = foreignKeyCol.fullName();				
				if(foreignKey!=null){
					Map<String, String> foreignKeyValueMap = new HashMap<String, String>();
					foreignKeyValueMap.put(foreignKey, value);
					ArrayList<Long> timestamps;
					if(finalResult.containsKey(foreignKeyValueMap)){
						timestamps = (ArrayList<Long>) finalResult.get(foreignKeyValueMap);
						timestamps.add(timestamp);
						finalResult.put(foreignKeyValueMap, timestamps);
					}else{
						timestamps = new ArrayList<Long>();
						timestamps.add(timestamp);
						finalResult.put(foreignKeyValueMap, timestamps);	
					}					
				}						
			}
		}
		
		Long maxDifferenceInAccessToRelatedTuples = (long) 0;
		Long minDifferenceInAccessToRelatedTuples = (long) 999999999;

		Iterator<List<Long>> iterator = finalResult.values().iterator();
		int overallDiskReadsOfRelatedTuples = 0;
		while(iterator.hasNext()){
			List<Long> timestamps = iterator.next();
			Long min = (long) 999999999;
			Long max = (long) 0;
			for(Long t:timestamps){
				if(t < min){
					min = t;
				}
				if(t > max){
					max = t;
				}
			}
			if((max - min) > maxDifferenceInAccessToRelatedTuples)
				maxDifferenceInAccessToRelatedTuples = max-min;
			if((max - min) < minDifferenceInAccessToRelatedTuples)
				maxDifferenceInAccessToRelatedTuples = max-min;
			overallDiskReadsOfRelatedTuples+= timestamps.size();
		}
		int possibleGroupings = finalResult.size();
		System.out.println("we could have grouped "+possibleGroupings + " out of "+overallDiskReadsOfRelatedTuples + " disk reads!!");
		System.out.println("Max interval of access between the grouped records is "+maxDifferenceInAccessToRelatedTuples/(1000*1000*1000) + " seconds");
		System.out.println("Min interval of access between the grouped records is "+minDifferenceInAccessToRelatedTuples/(1000*1000) + " milliseconds");
		System.out.println("workload duration is approx "+duration/(1000*1000*1000) + " seconds");
		return possibleGroupings;
	}

}
