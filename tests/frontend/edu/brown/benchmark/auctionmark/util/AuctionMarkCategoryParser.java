package edu.brown.benchmark.auctionmark.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;


public class AuctionMarkCategoryParser {
	
	Map<String, Category> _categoryMap;
	private int _nextCategoryID;
	String _fileName;

	public AuctionMarkCategoryParser(File file) {
	
		_categoryMap = new TreeMap<String, Category>();
		_nextCategoryID = 0;
		
		try {
			FileInputStream fstream = new FileInputStream(file);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {		
				extractCategory(strLine);
				//System.out.println(strLine);
			}
			in.close();
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}

	}

	public void extractCategory(String s){
		String[] tokens = s.split("\t");
		int itemCount = Integer.parseInt(tokens[5]);
		String categoryName = "";
		
		for(int i=0; i<=4; i++){
			if(!tokens[i].equals("")){
				categoryName += tokens[i] + "/";	
			} else {
				break;
			}
		}
		
		if(categoryName.length() > 0){
			categoryName = categoryName.substring(0, categoryName.length() - 1);
		}
		
		addNewCategory(categoryName, itemCount, true);
	}
	
	public Category addNewCategory(String fullCategoryName, int itemCount, boolean isLeaf){
		Category category = null;
		Category parentCategory = null;
		
		String categoryName = fullCategoryName;
		String parentCategoryName = "";
		int parentCategoryID = -1;
		
		if(categoryName.indexOf('/') != -1){
			int separatorIndex = fullCategoryName.lastIndexOf('/');
			parentCategoryName = fullCategoryName.substring(0, separatorIndex);
			categoryName = fullCategoryName.substring(separatorIndex + 1, fullCategoryName.length());
		}
		/*
		System.out.println("parentCat name = " + parentCategoryName);
		System.out.println("cat name = " + categoryName);
		*/
		if(_categoryMap.containsKey(parentCategoryName)){
			parentCategory = _categoryMap.get(parentCategoryName);
		} else if(!parentCategoryName.equals("")){
			parentCategory = addNewCategory(parentCategoryName, 0, false);
		}
		
		if(parentCategory!=null){
			parentCategoryID = parentCategory.getCategoryID();	
		}
		
		category = new Category(_nextCategoryID++,
		        categoryName,
				parentCategoryID, 
				itemCount, 
				isLeaf);
		
		_categoryMap.put(fullCategoryName, category);
		
		return category;
	}
	
	public Map<String, Category> getCategoryMap(){
		return _categoryMap;
	}
	
	public static void main(String args[]) throws Exception {	
		AuctionMarkCategoryParser ebp = new AuctionMarkCategoryParser(new File("bin/edu/brown/benchmark/auctionmark/data/categories.txt"));
		
		for(String key : ebp.getCategoryMap().keySet()){
			System.out.println(key + " : " + ebp.getCategoryMap().get(key).getCategoryID() + " : " + ebp.getCategoryMap().get(key).getParentCategoryID() + " : " + ebp.getCategoryMap().get(key).getItemCount());
		}
		//addNewCategory("001/123456/789", 0, true);
	}
}
