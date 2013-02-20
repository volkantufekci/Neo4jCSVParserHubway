package com.volkan.accesspattern;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import com.volkan.Configuration;

public class MainAccessPattern {

	private static final int RANDOM_ACCESS_COUNT = 10;
	private static final int MAX_NODE_COUNT 	 = 1850065;
	private static final int PARTITION_COUNT 	 = 10;
	private static final int LAST_PARTITION		 = 6483;
	private static int maxNodeCountInDBAP 		 = 0;

	private static final String DB_PATH = System.getProperty("user.home") +  
			"/Development/tez/Neo4jSurumleri/neo4j-community-1.8.M07erdos/data/graph.db/";
//			"/erdos8474notindexed.201301151430.graph.db/";
	private static GraphDatabaseService db, dbAP;
	private static final String refKeyName = "hashCode";
	private static final String refIndexName = "refNodes";
	
	private static final String allRefIndexName = "allRefNodes";
	private static final String allNormalNodeIndexName = "allNormalNodes";
	
	private static final String nodeKeyName = "gid";
	private static final String normalNodeIndexName = "nodes";

	private static Index<Node> allRefIndex;
	private static Index<Node> refNodeIndex;
	private static Index<Node> allNormalNodeIndex;
	private static Index<Node> normalNodeIndex;
	
	private static Set<String> cache = new HashSet<>();
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Runtime.getRuntime().exec("rm -rf "+Configuration.DB_AP_PATH);
		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		dbAP = new GraphDatabaseFactory().newEmbeddedDatabase(Configuration.DB_AP_PATH);
		registerShutdownHook();
		
		allRefIndex = dbAP.index().forNodes( allRefIndexName );
		refNodeIndex = dbAP.index().forNodes( refIndexName );
		allNormalNodeIndex = dbAP.index().forNodes( allNormalNodeIndexName );
		normalNodeIndex = dbAP.index().forNodes( normalNodeIndexName );
				
		createRandomAccessPatterns();
		operateGparting();
	}

	private static void createRandomAccessPatterns() throws IOException {
		List<Integer> randomIDs = createRandomIDs();
//		List<Integer> randomIDs = Arrays.asList(1527971,1824940);
		
		writeRandomIDs(randomIDs);
		
		TraversalDescription traversalDescription = createTraversalDesc();
		int i = 0;
		for (Integer randomID : randomIDs) {
			System.out.println("islenen randomID: " + randomID + " count: " + ++i);
			Node startNode = db.getNodeById(randomID);
			
			SortedSet<Long> set = putNodeIDsInPathIntoSet(traversalDescription, startNode);
			
			String hashCode = generateHashCodeOfNodeIDsInPath(set);
			if (!cache.contains(hashCode)) {
				cache.add(hashCode);
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
				} finally {
					tx.finish();
				}
			} else {
				System.out.println("PASSS");
			}
		}
	}

	private static SortedSet<Long> putNodeIDsInPathIntoSet(
			TraversalDescription traversalDescription, Node startNode) {
		SortedSet<Long> set = new TreeSet<>(); 
		for (Path path : traversalDescription.traverse(startNode)) {
			for (Node node : path.nodes()) {
				set.add(node.getId());
			}
		}
		return set;
	}
	
	private static Node createRefNodeAndAddToIndex(String hashCode, int randomID) {
		maxNodeCountInDBAP++;
		Node refNode;
		refNode = dbAP.createNode();
		refNode.setProperty(refKeyName, hashCode);
		refNode.setProperty("randomID", randomID);
		
		refNodeIndex.add(refNode, refKeyName, hashCode);
		allRefIndex.add(refNode, allRefIndexName, allRefIndexName);
		return refNode;
	}
	
	private static void createNodesInPathIfNeededAndConnectToRefNode(
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

	private static Node createNormalNodeAndAddToIndex(String propertyValue) {
		maxNodeCountInDBAP++;
		Node node;
		node = dbAP.createNode();
		node.setProperty(nodeKeyName, propertyValue);
		normalNodeIndex.add(node, nodeKeyName, propertyValue);
		allNormalNodeIndex.add(node, allNormalNodeIndexName, allNormalNodeIndexName);
		return node;
	}

	private static String generateHashCodeOfNodeIDsInPath(SortedSet<Long> set) {
		StringBuilder sb = new StringBuilder();
		for (Long id : set) {
			sb.append(id).append(",");
		}
		
//		System.out.println(sb.toString());
		String hashCode = sb.toString().hashCode() + "";
		return hashCode;
	}

	private static TraversalDescription createTraversalDesc() {
		TraversalDescription traversalDescription = 
				Traversal.description().evaluator(Evaluators.atDepth(2))
				.relationships(DynamicRelationshipType.withName("follows"), Direction.OUTGOING)
				.uniqueness(Uniqueness.NONE);
		return traversalDescription;
	}

	private static List<Integer> createRandomIDs() {
		Random random = new Random();
		//1850065ten kucuk 100K random sayi uret
		List<Integer> randomIDs = new ArrayList<Integer>(100);
		for (int i = 0; i < RANDOM_ACCESS_COUNT; i++) {
			int randomID = random.nextInt(MAX_NODE_COUNT) + 1;
			randomIDs.add(randomID);
		}
		return randomIDs;
	}
	
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
	
	public static void writeRandomIDs(List<Integer> randomIDs) throws IOException {

		BufferedWriter relFile = 
				new BufferedWriter(new FileWriter(Configuration.RANDOM_ID_FILE));
		
		for (Integer randomID : randomIDs) {
			relFile.write(randomID + "\n");
		}

		if (relFile != null)
			relFile.close();
	}
	
	private static void registerShutdownHook() {
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
	
	private static void operateGparting() throws IOException, InterruptedException {
		Neo4jClientForAccessPattern neo4jClient = new Neo4jClientForAccessPattern();
		Map<Long, List<Long>> nodeIDNeiIDArrayMap = 
								neo4jClient.collectNodeIDNeiIDsMap(dbAP, maxNodeCountInDBAP);
		
		GPartPartitioner.buildGrfFile(nodeIDNeiIDArrayMap);
		GPartPartitioner.performGpartingAndWriteGidPartitionMap(
				PARTITION_COUNT, neo4jClient.getNodeIDGidMap(), LAST_PARTITION, MAX_NODE_COUNT);
		
	}
}
