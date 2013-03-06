package com.volkan.accesspattern;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.Configuration;
import com.volkan.interpartitiontraverse.JsonHelper;

public class MainAccessPatternWoScotch extends MainAccessPatternWeight{

	private static final Logger logger = LoggerFactory.getLogger(MainAccessPatternWoScotch.class);
	
	Map<Integer, SortedSet<Long>> partitionGidsMap;
	
	public static void main(String[] args) throws Exception {
		MainAccessPatternWoScotch mainAccessPatternWoScotch = new MainAccessPatternWoScotch();
		mainAccessPatternWoScotch.work();
	}
	
	public MainAccessPatternWoScotch() {
		super();
		partitionGidsMap = new HashMap<Integer, SortedSet<Long>>();
	}
	
	public void work() throws Exception {
		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook();
		
		boolean useExistingAccessPatterns = true;
		
		// RECOMMENDATION
		Map<String, Object> jsonMap = JsonHelper
				.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "IN", "OUT"),
						Arrays.asList("follows", "follows", "follows"));
		String directory = "src/main/resources/jsons/erdos/100/3depth/";
		String ending = "out_in_out.json";
		usePreCalculatedIDsForJsons(3, jsonMap, directory, ending);
		
		createOrUseExistingAPs(jsonMap, directory, ending,
				useExistingAccessPatterns);

		// //2 Depths
		jsonMap = JsonHelper
				.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "OUT"),
						Arrays.asList("follows", "follows"));
		directory = "src/main/resources/jsons/erdos/100/2depth/";
		ending = "out_out.json";
		usePreCalculatedIDsForJsons(2, jsonMap, directory, ending);
		
		createOrUseExistingAPs(jsonMap, directory, ending,
				useExistingAccessPatterns);
		writeGidPartitionMapForRuby();
		
		writeGidPartitionMapForRubyForLastPartition();
	}
	
	protected void usePreCalculatedIDsForJsons(
			int depth, Map<String, Object> jsonMap, 
				String directory, String ending) throws IOException, Exception
	{
		Map<Integer,List<Integer>> depthIDsMap = new HashMap<>();
		List<Integer> depth2 = 
				Arrays.asList(5, 10, 15, 25, 30, 35, 40, 55, 60, 70, 65, 75, 80, 90, 
					100, 110, 125, 120, 140, 130, 135, 1, 6, 16, 21, 31, 36, 41, 46, 
					51, 56, 61, 71, 66, 76, 81, 91, 101, 106, 126, 121, 136, 131, 2, 7, 12, 17, 27, 
					37, 47, 52, 57, 67, 72, 87, 92, 102, 97, 107, 117, 122, 137, 132, 8, 13, 38, 
					43, 48, 53, 58, 63, 68, 78, 73, 83, 93, 103, 98, 118, 123, 138, 133, 4, 9, 14, 
					39, 44, 49, 54, 59, 69, 64, 79, 74, 89, 119, 124, 139, 129, 134);
		depthIDsMap.put(2, depth2);
		
		List<Integer> depth3 = 
				Arrays.asList(11, 394, 125, 234, 187, 416, 183, 382, 120, 95);
		depthIDsMap.put(3, depth3);
		
		//DELETES EVERYTHING UNDER directory
		createJsonOutputDir(directory);
		for (Integer id : depthIDsMap.get(depth)) {
			writeJsonToFile(jsonMap, directory, ending, id);
		}
	}
	
	protected void processRandomID(TraversalDescription traversalDescription, 
			Integer randomID, int count) throws Exception
	{
		SortedSet<Long> nodesInTraversal = 
				collectConnectedNodeIDsOfStartNodeID(randomID, traversalDescription);
		logger.info("randomID: "+randomID+" node count in traversal="
						+nodesInTraversal.size()+" count: "+ count);	
		
		int partition = randomID % (PARTITION_COUNT) + 6474;
		SortedSet<Long> gids = partitionGidsMap.get(partition);
		if (gids == null) {
			gids = new TreeSet<Long>();
			partitionGidsMap.put(partition, gids);
		}
		gids.add(randomID.longValue());
		gids.addAll(nodesInTraversal);
	}
	
	/**
	 * Write gid_partition_h_X for partitions except LAST_PARTITION
	 * @throws IOException
	 */
	private void writeGidPartitionMapForRuby() throws IOException {
		for (Integer partition : partitionGidsMap.keySet()) {
			String fileName = Configuration.GID_PARTITION_MAP + "_" + partition;
			BufferedWriter gpartInputFile = new BufferedWriter(new FileWriter(fileName));
			
			SortedSet<Long> gids = partitionGidsMap.get(partition);
			for (Long gid : gids) {
				gpartInputFile.write(gid+","+partition+"\n");
			}
			
			assignMissingGidsToLastPartition(gpartInputFile, gids);
			
			if (gpartInputFile != null)
				gpartInputFile.close();
		}
	}

	protected void assignMissingGidsToLastPartition(
			BufferedWriter gpartInputFile, SortedSet<Long> gids) throws IOException 
	{
		for (long i = 1; i <= MAX_NODE_COUNT; i++) {
			if (!gids.contains(i)) {
				gpartInputFile.write(i+","+LAST_PARTITION+"\n");
			}
		}
	}

	/**
	 * Writes gid_partition_h_X just for the last partition
	 * @throws IOException
	 */
	private void writeGidPartitionMapForRubyForLastPartition() throws IOException{
		String fileName = Configuration.GID_PARTITION_MAP+"_"+LAST_PARTITION;
		BufferedWriter gpartInputFile = new BufferedWriter(new FileWriter(fileName));
		
		for (long i = 1; i <= MAX_NODE_COUNT; i++) {
			gpartInputFile.write(i+","+LAST_PARTITION+"\n");
		}
		
		if (gpartInputFile != null)
			gpartInputFile.close();
	}
}
