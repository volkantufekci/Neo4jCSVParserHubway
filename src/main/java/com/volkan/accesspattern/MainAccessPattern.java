package com.volkan.accesspattern;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.Configuration;
import com.volkan.Utility;
import com.volkan.helpers.FileHelper;
import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.JsonKeyConstants;
import com.volkan.interpartitiontraverse.TraversalDescriptionBuilder;

public class MainAccessPattern {

	private static final Logger logger = LoggerFactory.getLogger(MainAccessPattern.class);
	
	protected int RANDOM_ACCESS_COUNT;
	protected int MAX_NODE_COUNT;
	protected int PARTITION_COUNT;
	protected int LAST_PARTITION;
	private  int maxNodeCountInDBAP 		 = 0;

	protected  String DB_PATH;
	protected GraphDatabaseService db, dbAP;

	private  final String refKeyName = "hashCode";
	private  final String refIndexName = "refNodes";
	
	private  final String allRefIndexName = "allRefNodes";
	private  final String allNormalNodeIndexName = "allNormalNodes";
	
	private  final String nodeKeyName = "gid";
	private  final String normalNodeIndexName = "nodes";

	private  Index<Node> allRefIndex;
	private  Index<Node> refNodeIndex;
	private  Index<Node> allNormalNodeIndex;
	private  Index<Node> normalNodeIndex;
	
	private static Set<String> cache = new HashSet<>();
	
	
	public MainAccessPattern() {
		DB_PATH = Utility.getValueOfProperty("erdosTekParcaDB_PATH", 
				"/erdos8474notindexed.201301151430.graph.db/");
		RANDOM_ACCESS_COUNT = Integer.parseInt(Utility.getValueOfProperty("RANDOM_ACCESS_COUNT", "0"));
		PARTITION_COUNT = Integer.parseInt(Utility.getValueOfProperty("PARTITION_COUNT", "0"));
		LAST_PARTITION = Integer.parseInt(Utility.getValueOfProperty("LAST_PARTITION", "6383"));
		MAX_NODE_COUNT = Integer.parseInt(Utility.getValueOfProperty("MAX_NODE_COUNT", "1850065"));
	}
	
	public static void main(String[] args) throws Exception {
		MainAccessPattern mainAccessPattern = new MainAccessPattern();
		mainAccessPattern.work();
	}

	public void work() throws Exception{
		Runtime.getRuntime().exec("rm -rf "+Configuration.DB_AP_PATH);
		Thread.sleep(5000);
		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		dbAP = new GraphDatabaseFactory().newEmbeddedDatabase(Configuration.DB_AP_PATH);
		registerShutdownHook();
		
		allRefIndex = dbAP.index().forNodes( allRefIndexName );
		refNodeIndex = dbAP.index().forNodes( refIndexName );
		allNormalNodeIndex = dbAP.index().forNodes( allNormalNodeIndexName );
		normalNodeIndex = dbAP.index().forNodes( normalNodeIndexName );
				
		//RECOMMENDATION
//		Map<String, Object> jsonMap = 
//				JsonHelper.createJsonMapWithDirectionsAndRelTypes(
//						Arrays.asList("OUT", "IN", "OUT"), 
//						Arrays.asList("follows", "follows", "follows"));
//		String jsonsOutputDir 	= "src/main/resources/jsons/erdos/3depth/";
//		String ending		  	= "out_in_out.json";
//		createJsonOutputDir(jsonsOutputDir);
//		createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);

//		//2 Depths
		Map<String, Object> jsonMap = JsonHelper.createJsonMapWithDirectionsAndRelTypes(
						Arrays.asList("OUT", "IN"), Arrays.asList("follows", "follows"));
		String jsonsOutputDir = "src/main/resources/jsons/erdos/2depth/";
		String ending		  = "out_out.json";
		createJsonOutputDir(jsonsOutputDir);
		createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);
		
//		jsonMap = JsonHelper.createJsonMapWithDirectionsAndRelTypes(
//						Arrays.asList("OUT", "IN"), Arrays.asList("follows", "follows"));
//		ending		  = "out_in.json";
//		createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);
//		
//		jsonMap = JsonHelper.createJsonMapWithDirectionsAndRelTypes(
//						Arrays.asList("IN", "IN"), Arrays.asList("follows", "follows"));
//		ending = "in_in.json";
//		createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);
//		
//		//FOLLOWERS
//		jsonMap = JsonHelper.createJsonMapWithDirectionsAndRelTypes(
//						Arrays.asList("IN"), Arrays.asList("follows"));
//		jsonsOutputDir 	= "src/main/resources/jsons/erdos/1depth/";
//		createJsonOutputDir(jsonsOutputDir);
//		ending = "in.json";
//		createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);
//		
//		//FRIENDS
//		jsonMap = JsonHelper.createJsonMapWithDirectionsAndRelTypes(
//				Arrays.asList("OUT"), Arrays.asList("follows"));
//		ending = "out.json";
//		createRandomAccessPatterns(jsonMap, jsonsOutputDir, ending);		
		
		operateGparting();
	}
	
	protected void createJsonOutputDir(String jsonsOutputDir) {
		logger.info(jsonsOutputDir+" is created if not existing");
		File f = new File(jsonsOutputDir);
		f.mkdirs();
		FileHelper.deleteFilesUnderFile(f);
	}

	private void createRandomAccessPatterns(
			Map<String, Object> jsonMap, String directory, String ending) throws Exception 
	{
		logger.info("Creating random access patterns for json:\n" +jsonMap
				+"\n in dir:"+directory+" with ending "+ending);
		TraversalDescription traversalDescription = 
				TraversalDescriptionBuilder.buildFromJsonMap(jsonMap);
		
		Set<Integer> randomIDSet = createRandomIDs();
		int i = 0;
		for (Integer randomID : randomIDSet) {
			SortedSet<Long> set = 
					collectConnectedNodeIDsOfStartNodeID(randomID, traversalDescription);
			System.out.println("randomID: "+randomID+" size="+set.size()+" count: "+ ++i);	
			if (nodeSetSizeIsTooBigForTests(set)) {
				System.out.println("nodeSetSizeIsTooBigForTests, PASSS");
				continue;
			}
			
			String hashCode = generateHashCodeOfNodeIDsInPath(set);
			if (!cache.contains(hashCode)) {
				cache.add(hashCode);
				createNodesInDBAP(randomID, set, hashCode);
				writeJsonToFile(jsonMap, directory, ending, randomID);
			} else {
				System.out.println("PASSS");
			}
		}
	}

	private boolean nodeSetSizeIsTooBigForTests(SortedSet<Long> set) {
		return set.size() > 10_000;
	}

	protected void writeJsonToFile(Map<String, Object> jsonMap,
			String directory, String ending, Integer randomID)
			throws IOException, Exception {
		jsonMap.put(JsonKeyConstants.START_NODE, randomID);
		FileHelper.writeToFile(
				JsonHelper.writeMapIntoJsonString(jsonMap), 
				directory+"/"+randomID+ending);
	}

	private  void createNodesInDBAP(
			Integer randomID, SortedSet<Long> set, String hashCode) throws Exception 
	{
		Transaction tx = dbAP.beginTx();
		try {
			Node refNode = refNodeIndex.get(refKeyName, hashCode).getSingle();
			if (refNode == null) {
				refNode = createRefNodeAndAddToIndex(hashCode, randomID);
				createNodesInPathIfNeededAndConnectToRefNode(set, refNode);
			}
			
			tx.success();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.toString());
		} finally {
			tx.finish();
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
		}
		logger.debug("nei count for {}={}", startNode.getId(), set.size());
		return set;
	}
	
	private Node createRefNodeAndAddToIndex(String hashCode, int randomID) {
		maxNodeCountInDBAP++;
		Node refNode;
		refNode = dbAP.createNode();
		refNode.setProperty(refKeyName, hashCode);
		refNode.setProperty("randomID", randomID);
		
		refNodeIndex.add(refNode, refKeyName, hashCode);
		allRefIndex.add(refNode, allRefIndexName, allRefIndexName);
		return refNode;
	}
	
	private void createNodesInPathIfNeededAndConnectToRefNode(
			SortedSet<Long> set, Node refNode) 
	{
		for (Long id : set) {
			Node node = normalNodeIndex.get(nodeKeyName, id+"").getSingle();
			if (node == null) {
				node = createNormalNodeAndAddToIndex(id + "");
			}
			
			refNode.createRelationshipTo(node, RelTypes.follows);
		}
	}

	private Node createNormalNodeAndAddToIndex(String propertyValue) {
		maxNodeCountInDBAP++;
		Node node;
		node = dbAP.createNode();
		node.setProperty(nodeKeyName, propertyValue);
		normalNodeIndex.add(node, nodeKeyName, propertyValue);
		allNormalNodeIndex.add(node, allNormalNodeIndexName, allNormalNodeIndexName);
		return node;
	}

	protected String generateHashCodeOfNodeIDsInPath(SortedSet<Long> set) {
		StringBuilder sb = new StringBuilder();
		for (Long id : set) {
			sb.append(id).append(",");
		}
		
		String hashCode = sb.toString().hashCode() + "";
		return hashCode;
	}

	protected Set<Integer> createRandomIDs() {
		Random random = new Random();
		//1850065ten kucuk 100K random sayi uret
		Set<Integer> randomIDs = new HashSet<>();
		for (int i = 0; i < RANDOM_ACCESS_COUNT; i++) {
			int randomID = random.nextInt(MAX_NODE_COUNT) + 1;
			randomIDs.add(randomID);
		}
//		randomIDs.add(1519527);
		return randomIDs;
	}
	
	/** Taken from Neo4j docs, not used
	 * @param propertyValue
	 * @param graphDb
	 * @param indexName
	 * @param keyName
	 * @return
	 */
	public static Node getOrCreateUserWithUniqueFactory( 
			String propertyValue, GraphDatabaseService graphDb, String indexName, final String keyName )
	{
	    UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory( graphDb, indexName )
	    {
	        @Override
	        protected void initialize( Node created, Map<String, Object> properties )
	        {
	            created.setProperty( keyName, properties.get( keyName ) );
	        }
	    };
	 
	    return factory.getOrCreate( keyName, propertyValue );
	}
	
	public void writeRandomIDs(List<Integer> randomIDs) throws IOException {

		BufferedWriter relFile = 
				new BufferedWriter(new FileWriter(Configuration.RANDOM_ID_FILE));
		
		for (Integer randomID : randomIDs) {
			relFile.write(randomID + "\n");
		}

		if (relFile != null)
			relFile.close();
	}
	
	private void registerShutdownHook() {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running example before it's completed)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				db.shutdown();
				dbAP.shutdown();
			}
		});
	}

	private static enum RelTypes implements RelationshipType
	{
	    follows
	}
	
	private void operateGparting() throws IOException, InterruptedException {
		Neo4jClientForAccessPattern neo4jClient = new Neo4jClientForAccessPattern();
		Map<Long, List<Long>> nodeIDNeiIDArrayMap = 
								neo4jClient.collectNodeIDNeiIDsMap(dbAP, maxNodeCountInDBAP);
		
		GPartPartitioner.buildGrfFile(nodeIDNeiIDArrayMap);
		GPartPartitioner.performGpartingAndWriteGidPartitionMap(
				PARTITION_COUNT, neo4jClient.getNodeIDGidMap(), LAST_PARTITION, MAX_NODE_COUNT);
		
	}
}
