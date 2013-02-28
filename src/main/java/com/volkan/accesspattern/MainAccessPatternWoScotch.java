package com.volkan.accesspattern;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.graphdb.traversal.TraversalDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.Configuration;
import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.TraversalDescriptionBuilder;

public class MainAccessPatternWoScotch extends MainAccessPattern{

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
//		//2 Depths
		Map<String, Object> jsonMap = JsonHelper.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "IN"), Arrays.asList("follows", "follows"));
		String jsonsOutputDir = "src/main/resources/jsons/erdos/2depth/";
		String ending		  = "out_out.json";
		createJsonOutputDir(jsonsOutputDir);
		createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);
		writeGidPartitionMapForRuby();
	}
	
	private void createRandomAccessPatterns(
			Map<String, Object> jsonMap, String directory, String ending) throws Exception 
	{
		logger.info("Creating random access patterns for json:\n" +jsonMap
				+"\n in dir:"+directory+" with ending "+ending);
		TraversalDescription traversalDescription = 
				TraversalDescriptionBuilder.buildFromJsonMapForAP(jsonMap);
		
		Set<Integer> randomIDSet = createRandomIDs();
		int i = 0;
		for (Integer randomID : randomIDSet) {
			SortedSet<Long> set = 
					collectConnectedNodeIDsOfStartNodeID(randomID, traversalDescription);
			logger.info("randomID: "+randomID+" size="+set.size()+" count: "+ ++i);	
			
			int partition = randomID % (PARTITION_COUNT-1) + 6474;
			SortedSet<Long> gids = partitionGidsMap.get(partition);
			if (gids == null) {
				gids = new TreeSet<Long>();
				partitionGidsMap.put(partition, gids);
			}
			gids.add(randomID.longValue());
			gids.addAll(set);
			
		}
	}
	
	private void writeGidPartitionMapForRuby() throws IOException {
		for (Integer partition : partitionGidsMap.keySet()) {
			String fileName = Configuration.GID_PARTITION_MAP + "_" + partition;
			BufferedWriter gpartInputFile = new BufferedWriter(new FileWriter(fileName));
			
			for (Long gid : partitionGidsMap.get(partition)) {
				gpartInputFile.write(gid+","+partition+"\n");
			}
			
			if (gpartInputFile != null)
				gpartInputFile.close();
		}
	}

}
