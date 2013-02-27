package com.volkan.accesspattern;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.TraversalDescriptionBuilder;

public class MainAccessPatternWeight extends MainAccessPattern {

	private static final Logger logger = LoggerFactory.getLogger(MainAccessPatternWeight.class);

	private static final int EDGE_WEIGHT = 5;
	private static final String DB_PATH = System.getProperty("user.home") +  
			"/Development/tez/Neo4jSurumleri/neo4j-community-1.8.M07erdos/data/graph.db/";
//			"/erdos8474notindexed.201301151430.graph.db/";
	protected static GraphDatabaseService db;

	public static void main(String[] args) throws Exception {

		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook();
		
		//RECOMMENDATION
		Map<String, Object> jsonMap = 
				JsonHelper.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "IN", "OUT"), 
						Arrays.asList("follows", "follows", "follows"));
		String jsonsOutputDir 	= "src/main/resources/jsons/erdos/3depth/";
		String ending		  	= "out_in_out.json";
		createJsonOutputDir(jsonsOutputDir);
		createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);
		operateGparting();
	}
	
	private static void createRandomAccessPatterns(
			Map<String, Object> jsonMap, String directory, String ending) throws Exception 
	{
		logger.info("Creating random access patterns for json:\n" +jsonMap
				+"\n in dir:"+directory+" with ending "+ending);
		TraversalDescription traversalDescription = 
				TraversalDescriptionBuilder.buildFromJsonMapForAP(jsonMap);
		
		Set<Integer> randomIDSet = createRandomIDs();
		int i = 0;
		for (Integer randomID : randomIDSet) {
			Node startNode 		= db.getNodeById(randomID);
			increaseTraversedEdgesWeight(traversalDescription, startNode);
			
			writeJsonToFile(jsonMap, directory, ending, randomID);
			logger.debug("{} edge weight increased", ++i);
		}
	}
	
	private static void increaseTraversedEdgesWeight(TraversalDescription td, Node startNode) 
			throws Exception 
	{
		Transaction tx = db.beginTx();
		try {
			for (Path path : td.traverse(startNode)) {
				for(Relationship rel : path.relationships()){
					int weight = (int) rel.getProperty("weight", 0);
					rel.setProperty("weight", weight + EDGE_WEIGHT);
				}
				logger.debug(path.toString());
			}
			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.toString());
		} finally {
			tx.finish();
		}
	}
	
	private static void operateGparting() throws IOException, InterruptedException {
		Neo4jClientForAccessPattern neo4jClient = new Neo4jClientForAccessPattern();
		Map<Long, List<EdgeWithWeight>> nodeIDNeiIDArrayMap = 
					neo4jClient.collectNodeIDWeightedEdgeArrayMap(db, MAX_NODE_COUNT);
		
		GPartPartitionerWeighted.buildGrfFile(nodeIDNeiIDArrayMap);
		GPartPartitionerWeighted.performGpartingAndWriteGidPartitionMap(PARTITION_COUNT);
	}

	private static void registerShutdownHook() {
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
