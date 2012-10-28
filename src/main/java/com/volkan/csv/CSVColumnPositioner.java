package com.volkan.csv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVColumnPositioner {

	private List<String> columns;

	public CSVColumnPositioner() {
		columns = new ArrayList<String>();
	}
	
	public void addColumns(String[] columnNames){
		for (String columnName : columnNames) {
			if (!columns.contains(columnName)) {
				columns.add(columnName);
			}
		}
		
		Collections.sort(columns);
	}

	public Map<String, Integer> columnOrdersFor
										(NodePropertyHolder nodePropertyHolder) {
		
		String[] properties = nodePropertyHolder.getNodePropertyNames();
		Map<String, Integer> hmColumnOrderSubset = new HashMap<String, Integer>();
		
		for (String property : properties) {
			hmColumnOrderSubset.put(property, columns.indexOf(property));
		}
		
		return hmColumnOrderSubset;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for (String property : columns) {
			sb.append(property + "\t");
		}
		sb.append("\n");
		return sb.toString();
	}

	public List<String> getColumns() {
		return columns;
	}
	
}
