package com.volkan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.JsonKeyConstants;
import com.volkan.interpartitiontraverse.RestConnector;
import com.volkan.interpartitiontraverse.ShadowEvaluator;
import com.volkan.interpartitiontraverse.TraverseHelper;

public class Main {

	private static final Logger logger = LoggerFactory.getLogger(Main.class);
//	private static final String DB_PATH = "./src/main/resources/graph.db";
//	private static final String DB_PATH = 
//			"/home/volkan/Development/tez/Neo4jSurumleri/neo4j-community-1.8.M07_eguller/data/graph.db";
	private static final String DB_PATH = "/home/volkan/erdos8474notindexed.201301151430.graph.db";
	private static GraphDatabaseService db;
	
	public static void main(String[] args) {

		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook();
		ExecutionEngine engine = new ExecutionEngine(db);
		
		long start = System.currentTimeMillis();
		int hede = 0;
		try {
			switch (hede) {
			case 0:
//				Neo4jClient.getNodesWithMostFriendsFromEgullerTwitterDB(engine);
//				Neo4jClient.myFriendDepth3(db);
//				ErdosWebGraphImporter.readBigFile();
				new Neo4jClient().getMostFollowedErdos(db);
				break;
//			case 1:
//				testMostUsedStations(engine);
//				break;
//			case 2:
//				testPrintingOrdersOfColumns(engine);
//				break;
//			case 3:
//				test10RelFetch(engine);
//				break;
//			case 4:
//				getMostUsedStationsConnections(engine);
//				break;
			case 5:
				traverseWithShadowEvaluator();
				break;
			case 6:
				traverseViaJsonMap(JsonHelper.readJsonFileIntoMap("testhop.json"));
				break;
			case 7:
				String port = "7474";
				String url	= "http://localhost";
				delegateQueryToAnotherNeo4j(url, port, JsonHelper.readJsonFileIntoMap("testhop.json"));
				break;
			case 9:
				new H2Client().generateJob(JsonHelper.readJsonFileIntoMap("testhop.json"));
				break;
			case 10:
				new H2Client().updateJobWithCypherResult(1l);
				break;
			case 11:
				H2Client h2Client = new H2Client();
				h2Client.deleteAll();
				long jobID = h2Client.generateJob(JsonHelper.readJsonFileIntoMap("testhopAsync.json"));

				Map<String, Object> jsonMap = JsonHelper.readJsonFileIntoMap("testhopAsync.json");
				jsonMap.put(JsonKeyConstants.JOB_ID, jobID);
				jsonMap.put(JsonKeyConstants.PARENT_JOB_ID, jobID);

				Neo4jClientAsync neo4jClientAsync = new Neo4jClientAsync();
				neo4jClientAsync.delegateQueryAsync("8474", jsonMap);
				neo4jClientAsync.periodicFetcher((long) jsonMap.get(JsonKeyConstants.PARENT_JOB_ID));

				break;
			case 12:
				new H2Client().readClob(32);
				break;
			default:
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		logger.info(end - start + " miliseconds passed");
	}

	
	private static void delegateQueryToAnotherNeo4j(String url, String port, 
													Map<String, Object> jsonMap) {	
		long start = System.currentTimeMillis();
		RestConnector restConnector = new RestConnector(url, port);
		String jsonString = restConnector.delegateQuery(jsonMap);
		List<String> resultList = JsonHelper.convertJsonStringToList(jsonString);
		for (String result : resultList) {
			logger.info(result);
		}
		
		long end = System.currentTimeMillis();
		logger.info(end - start + " miliseconds passed, " + "result.size= " + resultList.size());
	}

	
	private static void traverseViaJsonMap(Map<String, Object> jsonMap) {
		List<String> resultJson = new ArrayList<String>();

		TraverseHelper traverseHelper = new TraverseHelper();
		resultJson = traverseHelper.traverse(db, jsonMap);
		
		for (String result : resultJson) {
			System.out.println(result);
		}
	}

	private static void traverseWithShadowEvaluator() {
		Set<Node> shadowResults = new HashSet<Node>();
		Set<String> realResults 	= new HashSet<String>();
		
		Node node = db.getNodeById(52220);
		int i = 0; int toDepth = 3;
//		OrderedByTypeExpander mexpander = new OrderedByTypeExpander();
//		mexpander.add(DynamicRelationshipType.withName("BIKE"), Direction.OUTGOING);
		for (Path path : Traversal.description()
				.relationships(DynamicRelationshipType.withName("BIKE"), Direction.OUTGOING)
				.relationships(DynamicRelationshipType.withName("BIKE"), Direction.INCOMING)
				.relationships(DynamicRelationshipType.withName("END"), Direction.OUTGOING)
				.evaluator(Evaluators.fromDepth(1)).evaluator(Evaluators.toDepth(toDepth))
				.evaluator(new ShadowEvaluator())
				.traverse(node)) {
				Node endNode = path.endNode();
				if (path.length() < toDepth) {
//					String gid = (String) endNode.getProperty("Gid");
					shadowResults.add(endNode);
					System.out.println("id: " + endNode.getId() + "\t" 
										+ ShadowEvaluator.isShadow(endNode) + "\t" + path );
//					String port = (String) endNode.getProperty("Real");
//					delegateQueryToAnotherNeo4j(url, port, jsonMap);
				} else {
					realResults.add((String) endNode.getProperty("Name"));
				}
			i++;		
		}
		System.out.println(i);
		for (String name : realResults) {
			System.out.println(name);
		}
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