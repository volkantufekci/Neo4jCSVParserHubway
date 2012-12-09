package com.volkan;

public class Configuration {

	public static final String HUBWAY_STATIONS_CSV = System.getProperty("user.home") 
													+ "/hubway_original_csv_dir/stations.csv";
	
	public static final String HUBWAY_TRIPS_CSV = System.getProperty("user.home") 
												+ "/hubway_original_csv_dir/trips.csv";
	
	public static final String NODES_CSV = System.getProperty("user.home") 
			+ "/nodes.csv";
	
	public static final String RELS_CSV = System.getProperty("user.home") 
			+ "/rels.csv";
	
	public static final int MAX_NODE_COUNT = 553000;
}
