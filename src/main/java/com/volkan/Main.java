package com.volkan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.db.H2Helper;
import com.volkan.db.VJobEntity;
import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.JsonKeyConstants;
import com.volkan.interpartitiontraverse.RestConnector;
import com.volkan.interpartitiontraverse.ShadowEvaluator;
import com.volkan.interpartitiontraverse.TraverseHelper;

public class Main {

	private static final Logger logger = LoggerFactory.getLogger(Main.class);
//	private static final String DB_PATH = "/home/volkan/erdos8474notindexed.201301151430.graph.db";
	private static GraphDatabaseService db;
	
	public static void main(String[] args) {

//		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
//		registerShutdownHook();
//		ExecutionEngine engine = new ExecutionEngine(db);
		
		long start = System.currentTimeMillis();
		int hede = 13;
		try {
			switch (hede) {
			case 0:
//				Neo4jClient.getNodesWithMostFriendsFromEgullerTwitterDB(engine);
//				Neo4jClient.myFriendDepth3(db);
//				ErdosWebGraphImporter.readBigFile();
//				new Neo4jClient().getMostFollowedErdos(db);
				break;
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
			case 10:
				new H2Client().updateJobWithCypherResult(1l);
				break;
			case 11:
				H2Client h2Client = new H2Client();
				h2Client.deleteAll();
				String jsonFileName = "src/main/resources/jsons/erdos111111.json";
				Map<String, Object> jsonMap = generateJobFromJsonFileName(h2Client, jsonFileName);

				Neo4jClientAsync neo4jClientAsync = new Neo4jClientAsync();
				neo4jClientAsync.delegateQueryAsync("6474", jsonMap);
				neo4jClientAsync.periodicFetcher((long) jsonMap.get(JsonKeyConstants.PARENT_JOB_ID));

				break;
			case 12:
				List<VJobEntity> list = new H2Helper().fetchJobNotDeletedWithParentID(37l);
				List<Long> jobIDs = new ArrayList<>();
				for (VJobEntity vJobEntity : list) {
					System.out.println(vJobEntity.getId());
					jobIDs.add(vJobEntity.getId());
				}
				new H2Helper().updateJobsMarkAsDeleted(jobIDs);
				break;
			case 13:
				ExecutorService executorService = Executors.newFixedThreadPool(2);
				executorService.execute(new Runnable() {
					
					@Override
					public void run() {
						try {
							H2Client h2Client = new H2Client();
							h2Client.deleteAll();
							String jsonFileName = "src/main/resources/jsons/erdos6474_6.json";
							Map<String, Object> jsonMap = generateJobFromJsonFileName(h2Client, jsonFileName);

							Neo4jClientAsync neo4jClientAsync = new Neo4jClientAsync();
							neo4jClientAsync.delegateQueryAsync("6474", jsonMap);
							neo4jClientAsync.periodicFetcher((long) jsonMap.get(JsonKeyConstants.PARENT_JOB_ID));
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					}
				});
				
				System.out.println("6474 cagrildi");
				
				executorService.execute(new Runnable() {
					
					@Override
					public void run() {
						try {
							H2Client h2Client = new H2Client();
							h2Client.deleteAll();
							String jsonFileName = "src/main/resources/jsons/erdos6475_1.json";
							Map<String, Object> jsonMap = generateJobFromJsonFileName(h2Client, jsonFileName);

							Neo4jClientAsync neo4jClientAsync = new Neo4jClientAsync();
							neo4jClientAsync.delegateQueryAsync("6475", jsonMap);
							neo4jClientAsync.periodicFetcher((long) jsonMap.get(JsonKeyConstants.PARENT_JOB_ID));
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					}
				});
				
				System.out.println("6475 cagrildi");
				
				executorService.shutdown();
				while (!executorService.isTerminated()) {
					
				}
				
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


	private static Map<String, Object> generateJobFromJsonFileName(
			H2Client h2Client, String jsonFileName) throws Exception {
		Map<String, Object> jsonMap = JsonHelper.readJsonFileIntoMap(jsonFileName);
		long jobID = h2Client.generateJob(jsonMap);
		jsonMap.put(JsonKeyConstants.JOB_ID, jobID);
		jsonMap.put(JsonKeyConstants.PARENT_JOB_ID, jobID);
		return jsonMap;
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

	@SuppressWarnings("unused")
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