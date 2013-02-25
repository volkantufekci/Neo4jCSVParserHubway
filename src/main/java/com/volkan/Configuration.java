package com.volkan;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Configuration {

	public static final String HUBWAY_STATIONS_CSV = System.getProperty("user.home") 
													+ "/hubway_original_csv_dir/stations.csv";
	
	public static final String HUBWAY_TRIPS_CSV = System.getProperty("user.home") 
												+ "/hubway_original_csv_dir/trips.csv";
	
	public static final Path ERDOS_TSV = Paths.get(System.getProperty("user.home"), 
												"Downloads", "graph.txt"); 
	
	public static final String NODES_CSV = System.getProperty("user.home") 
			+ "/nodes.csv";
	
	public static final String RELS_CSV = System.getProperty("user.home") 
			+ "/rels.csv";
	
	public static final String RELS_TXT_VOSVIEWER = "relsVOS.txt";
	public static final String NET_FILE_PAJEK = "erdos.net";
	
	public static final String RANDOM_ID_FILE = "randomIDs";
	
	public static final int MAX_NODE_COUNT = 553000;
	
	public static final String BASE_URL_OF_NEO4J_INSTANCES = "http://localhost";
	
//	public static final String DB_AP_PATH = System.getProperty("user.home") +  
//			"/dbAccessPattern.graph.db/";
	public static final String DB_AP_PATH = "/mnt/v/dbAccessPattern.graph.db/";

	public static final String GPART_RESULT_PATH = System.getProperty("user.home") + 
			"/result.map";
	public static final String GPART_GRF_PATH = System.getProperty("user.home") + 
			"/gpartInputFile.grf";
	public static final String GID_PARTITION_MAP = System.getProperty("user.home") + 
			"/gid_partition_h";
}
