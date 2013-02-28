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

import com.volkan.helpers.FileListingVisitor;
import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.JsonKeyConstants;
import com.volkan.interpartitiontraverse.TraversalDescriptionBuilder;

public class MainAccessPatternWeight extends MainAccessPattern {

	private static final Logger logger = LoggerFactory.getLogger(MainAccessPatternWeight.class);

	private final int EDGE_WEIGHT = 5;
	
	protected GraphDatabaseService db;

	public static void main(String[] args) throws Exception {
		MainAccessPatternWeight mainAccessPatternWeight = new MainAccessPatternWeight();
		mainAccessPatternWeight.work();
	}
	
	public MainAccessPatternWeight() {
		super();
	}

	public void work() throws Exception {
		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook();
		
		//RECOMMENDATION
		Map<String, Object> jsonMap = 
				JsonHelper.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "IN", "OUT"), 
						Arrays.asList("follows", "follows", "follows"));
		String jsonsOutputDir 	= "src/main/resources/jsons/erdos/3depth/";
		String ending		  	= "out_in_out.json";
		
		boolean useExistingAccessPatterns = true;
		createOrUseExistingAPs(
			jsonMap, jsonsOutputDir, ending, useExistingAccessPatterns);
		
//		//2 Depths
		jsonMap = JsonHelper.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "IN"), Arrays.asList("follows", "follows"));
		jsonsOutputDir = "src/main/resources/jsons/erdos/2depth/";
		ending		  = "out_in.json";
		
		createOrUseExistingAPs(
			jsonMap, jsonsOutputDir, ending, useExistingAccessPatterns);
		
		operateGparting();
	}

	protected void createOrUseExistingAPs(Map<String, Object> jsonMap,
			String jsonsOutputDir, String ending,
			boolean useExistingAccessPatterns) throws Exception {
		if (useExistingAccessPatterns) {
			useExistingAccPattsFromJsons(jsonsOutputDir, ending);
		} else {
			createJsonOutputDir(jsonsOutputDir);
			createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);
		}
	}
	
	private void useExistingAccPattsFromJsons(String directory, String ending) 
			throws Exception 
	{
		List<String> fileNames = FileListingVisitor.listJsonFileNamesInDir(directory);
		if (fileNames.isEmpty()) {
			logger.error(directory+" does not exist or no json file exists");
		} else {
			int i = 0;
			for (String fileName : fileNames) {
				Map<String, Object> jsonMap = JsonHelper.readJsonFileIntoMap(fileName);
				Integer startNodeID = (Integer) jsonMap.get(JsonKeyConstants.START_NODE);
			
				logger.info("Creating random access patterns without existing json:\n" +jsonMap
						+"\n in dir:"+directory+" with ending "+ending);
				TraversalDescription traversalDescription = 
						TraversalDescriptionBuilder.buildFromJsonMapForAP(jsonMap);
				
				Node startNode 		= db.getNodeById(startNodeID);
				increaseTraversedEdgesWeight(traversalDescription, startNode);
				logger.info("{} edges weight increased", ++i);
			}
		}
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
			Node startNode 		= db.getNodeById(randomID);
			increaseTraversedEdgesWeight(traversalDescription, startNode);
			
			writeJsonToFile(jsonMap, directory, ending, randomID);
			logger.info("{} edges weight increased", ++i);
		}
	}
	
	private void increaseTraversedEdgesWeight(TraversalDescription td, Node startNode) 
			throws Exception 
	{
		logger.info("increaseTraversedEdgesWeight started");
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
	
	private void operateGparting() throws IOException, InterruptedException {
		Neo4jClientForAccessPattern neo4jClient = new Neo4jClientForAccessPattern();
		Map<Long, List<EdgeWithWeight>> nodeIDNeiIDArrayMap = 
					neo4jClient.collectNodeIDWeightedEdgeArrayMap(db, MAX_NODE_COUNT);
		
		GPartPartitionerWeighted.buildWeightedGrfFile(nodeIDNeiIDArrayMap);
		GPartPartitionerWeighted.performGpartingAndWriteGidPartitionMap(PARTITION_COUNT);
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
