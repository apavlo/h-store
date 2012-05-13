package edu.brown.hstore;

import java.util.*;

import org.voltdb.catalog.Catalog;

/* This is a class that holds a list of the table names for an instance of HStore */

public class HStoreDataList {
	
	final Catalog catalog;
	final List<String> hstore_data;
	
	public HStoreDataList(Catalog catalog) throws Exception{
		this.catalog = catalog;
		this.hstore_data = new ArrayList();		
		
		//Load up the array list 
		//TODO: change this to add all of the table names from the argument catalog
		this.hstore_data.add("SELECT");
		this.hstore_data.add("UPDATE");
		this.hstore_data.add("DELETE");
		this.hstore_data.add("INSERT");
	}
	
	public String[] getData(){
		String[] return_list = new String[this.hstore_data.size()];
		for(int i = 0; i<this.hstore_data.size(); i++){
			return_list[i] = this.hstore_data.get(i);
		}
		return return_list;
	}
	

}
