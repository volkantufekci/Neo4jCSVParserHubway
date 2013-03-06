package com.volkan.accesspattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.Utility;
import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.TraversalDescriptionBuilder;

public class RandomIDController {

	private static final Logger logger = LoggerFactory.getLogger(RandomIDController.class);

	private final int MAX_NODE_COUNT;
	private final int PARTITION_COUNT;
	
	protected GraphDatabaseService db;
	
	public RandomIDController(GraphDatabaseService db, int maxNodeCount, int partitionCount) {
		this.db = db;
		this.MAX_NODE_COUNT = maxNodeCount;
		this.PARTITION_COUNT = partitionCount;
		registerShutdownHook();
	}
	
	public static void main(String[] args) throws Exception {
		String DB_PATH = Utility.getValueOfProperty("erdosTekParcaDB_PATH","");
		GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH );
		
		int maxNodeCount = Integer.parseInt(Utility.getValueOfProperty("MAX_NODE_COUNT", "1850065"));
		int partitionCount = Integer.parseInt(Utility.getValueOfProperty("PARTITION_COUNT", "0"));
		RandomIDController randomIDController = 
				new RandomIDController(db, maxNodeCount, partitionCount);
		randomIDController.getMost2DepthErdos();
	}
	
	public void getMost3DepthErdos() throws Exception {
		Map<Long,Integer> gidNeiCount = new HashMap<>();
		
		Map<String, Object> jsonMap = JsonHelper
				.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "IN", "OUT"),
						Arrays.asList("follows", "follows", "follows"));
		
		fillGidNeiCountViaTraversing(gidNeiCount, jsonMap, 50);
	}
	

	public void getMost2DepthErdos() throws Exception {
		Map<Long,Integer> gidNeiCount = new HashMap<>();
		
		Map<String, Object> jsonMap = JsonHelper
				.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "OUT"),
						Arrays.asList("follows", "follows"));
		
		fillGidNeiCountViaTraversing(gidNeiCount, jsonMap, 100);
	}

	private void fillGidNeiCountViaTraversing(Map<Long, Integer> gidNeiCount,
			Map<String, Object> jsonMap, int desiredMaxCollectedCount) throws Exception {
		TraversalDescription traversalDescription = 
				TraversalDescriptionBuilder.buildFromJsonMap(jsonMap);
		for(int i = 1; i <= MAX_NODE_COUNT; i++){
			SortedSet<Long> neis = collectConnectedNodeIDsOfStartNodeID(i, traversalDescription);
			
			logger.info("gid={} neiSize={}", i, neis.size());
			
			if(100 <= neis.size() && neis.size() <= 50_000){
				gidNeiCount.put(new Integer(i).longValue(), neis.size());
			}
			if(gidNeiCount.size() >= desiredMaxCollectedCount){
				break;
			}
		}
		
		Map<Long,List<Long>> partitionGids = new HashMap<>();
		for (Long gid : gidNeiCount.keySet()) {
			System.out.println(gid+": "+gidNeiCount.get(gid)+"\n");
			Long partition = (gid % PARTITION_COUNT) + 6474;
			
			List<Long> gids = partitionGids.get(partition); 
			if (gids == null) {
				gids = new ArrayList<>();
				partitionGids.put(partition, gids);
			}
			gids.add(gid);
		}
		
		for (Long partition : partitionGids.keySet()) {
			logger.info("{}: {}", partition, partitionGids.get(partition));
		}
	}
	
	public SortedSet<Long> collectConnectedNodeIDsOfStartNodeID(
			Integer startNodeID, TraversalDescription traversalDescription) throws Exception 
	{
		Node startNode 		= db.getNodeById(startNodeID);
		SortedSet<Long> set = putNodeIDsInPathIntoSet(traversalDescription, startNode);
		return set;
	}
	
	private SortedSet<Long> putNodeIDsInPathIntoSet(
			TraversalDescription traversalDescription, Node startNode) throws Exception 
	{
		SortedSet<Long> set = new TreeSet<>(); 
		for (Path path : traversalDescription.traverse(startNode)) {
			for (Node node : path.nodes()) {
				set.add(node.getId());
			}
			logger.debug(path.toString());
			if (set.size() > 50_000) {
				//Already too big, no need to continue
				set = new TreeSet<>();
				break;
			}
		}
		logger.debug("nei count for {}={}", startNode.getId(), set.size());
		return set;
	}
	
	private void registerShutdownHook() {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running example before it's completed)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				db.shutdown();
			}
		});
	}
}
