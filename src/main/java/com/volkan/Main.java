package com.volkan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;

import com.volkan.db.H2Helper;
import com.volkan.db.VJobEntity;
import com.volkan.helpers.FileListingVisitor;
import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.JsonKeyConstants;
import com.volkan.interpartitiontraverse.RestConnector;
import com.volkan.interpartitiontraverse.ShadowEvaluator;
import com.volkan.interpartitiontraverse.TraverseHelper;

public class Main {

	private static final Logger logger = LoggerFactory.getLogger(Main.class);
//	private static final String DB_PATH = "/home/volkan/Development/tez/nodes_rels_csv_arsivi/erdos8474notindexed.201301151430.graph.db";
//	private static final String DB_PATH = "/home/volkan/Development/tez/nodes_rels_csv_arsivi/erdos6477graph.db/";
	private static GraphDatabaseService db;
	
	public static void main(String[] args) {

//		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
//		registerShutdownHook();
//		ExecutionEngine engine = new ExecutionEngine(db);
		
		long start = System.currentTimeMillis();
		int hede = 14;
		try {
			switch (hede) {
			case 0:
//				Neo4jClient.getNodesWithMostFriendsFromEgullerTwitterDB(engine);
//				Neo4jClient.myFriendDepth3(db);
//				ErdosWebGraphImporter.readBigFile();
				new Neo4jClient().getMostFollowedNodes(db, 432911, "follows", Direction.INCOMING);
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
				Map<String, Object> jsonMap = generateJobInDBFromJsonFileName(h2Client, jsonFileName);

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
				H2Client h2 = new H2Client();
				h2.deleteAll();
				logger.info("STARTED");
				start = System.currentTimeMillis();
				ExecutorService executorService = Executors.newFixedThreadPool(2);
				executorService.execute(new Runnable() {
					
					@Override
					public void run() {
						try {
							H2Client h2Client = new H2Client();
							String jsonFileName = "src/main/resources/jsons/erdos6474_6_143.json";
							Map<String, Object> jsonMap = generateJobInDBFromJsonFileName(h2Client, jsonFileName);

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
							String jsonFileName = "src/main/resources/jsons/erdos6475_1_21.json";
							Map<String, Object> jsonMap = generateJobInDBFromJsonFileName(h2Client, jsonFileName);

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
				
			case 14:
//				generate10Jobs();
				generateRandomJobs();
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

	private static void generate10Jobs() throws Exception {
		ExecutorService executorService = Executors.newFixedThreadPool(10);
		H2Client h2Client = new H2Client();
		
		h2Client.deleteAll();
		logger.info("deletedAll and STARTED");
		
		String jsonFileName = Utility.getValueOfProperty("jsonFileName", "-1");
		switch (jsonFileName) {
		case "6474":
			executeJobForPortForJsonFileName(
					"src/main/resources/jsons/erdos6474_6_143.json", 
					executorService, h2Client);
			break;
		case "6475":
			executeJobForPortForJsonFileName(
					"6475", "src/main/resources/jsons/erdos6475_1_21.json", 
					executorService, h2Client);
			break;
		case "6476":
			executeJobForPortForJsonFileName(
					"6476", "src/main/resources/jsons/erdos6476_3_90.json", 
					executorService, h2Client);
			break;
		case "6477":
			executeJobForPortForJsonFileName(
					"6477", "src/main/resources/jsons/erdos6477_446_27.json", 
					executorService, h2Client);
			break;
		case "6482":
			executeJobForPortForJsonFileName(
					"6482", "src/main/resources/jsons/erdos6482_85_1000.json", 
					executorService, h2Client);
			break;	
		default://Run ALL
			executeJobForPortForJsonFileName(
					"6474", "src/main/resources/jsons/erdos6474_6_143.json", 
					executorService, h2Client);
			executeJobForPortForJsonFileName(
					"6475", "src/main/resources/jsons/erdos6475_1_21.json", 
					executorService, h2Client);
			executeJobForPortForJsonFileName(
					"6476", "src/main/resources/jsons/erdos6476_3_90.json", 
					executorService, h2Client);
			executeJobForPortForJsonFileName(
					"6477", "src/main/resources/jsons/erdos6477_446_27.json", 
					executorService, h2Client);
			executeJobForPortForJsonFileName(
					"6478", "src/main/resources/jsons/erdos6478_138704_2902.json", 
					executorService, h2Client);
			executeJobForPortForJsonFileName(
					"6479", "src/main/resources/jsons/erdos6479_1402_330.json", 
					executorService, h2Client);		
			executeJobForPortForJsonFileName(
					"6480", "src/main/resources/jsons/erdos6480_73_515.json", 
					executorService, h2Client);
			executeJobForPortForJsonFileName(
					"6481", "src/main/resources/jsons/erdos6481_1238_82.json", 
					executorService, h2Client);
			executeJobForPortForJsonFileName(
					"6482", "src/main/resources/jsons/erdos6482_85_1000.json", 
					executorService, h2Client);
			executeJobForPortForJsonFileName(
					"6483", "src/main/resources/jsons/erdos6483_964_43.json", 
					executorService, h2Client);
			break;
		}
		
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			
		}
	}
	
	private static void generateRandomJobs() throws Exception {
		H2Client h2Client = new H2Client();
		h2Client.deleteAll();
		logger.info("deletedAll and STARTED");
		
		final JedisPool jedisPool = new JedisPool(Utility.getValueOfProperty("redisurl", null), 6379);
		
		String rootDir = Utility.getValueOfProperty("jsonRootDir", "");
		int totalRandomJobCount = Integer.parseInt(
				Utility.getValueOfProperty("totalRandomJobCount", Integer.MAX_VALUE + ""));
		List<String> jsonFileNames = 
				FileListingVisitor.listRandomJsonFileNamesInDir(rootDir, totalRandomJobCount);
		
		long interval 	 = Long.parseLong(Utility.getValueOfProperty("interval", "0"));
		long totalRunTime = Long.parseLong(Utility.getValueOfProperty("totalRunTime", "0"));
		
		ScheduledExecutorService executorService = Executors.newScheduledThreadPool(totalRandomJobCount);
		for (String fileName : jsonFileNames) {
			executeScheduledJobForJsonFileName(
					fileName, interval, totalRunTime, executorService, h2Client, jedisPool);
		}

		Thread.sleep(totalRunTime * 1000);
		executorService.shutdown();
	}

	private static void executeScheduledJobForJsonFileName(
			String jsonFileName, long interval, long totalRunTime,
				ScheduledExecutorService scheduler, H2Client h2Client, JedisPool jedisPool) 
					throws Exception {
		Map<String, Object> jsonMap = JsonHelper.readJsonFileIntoMap(jsonFileName);
		
		Runnable r	= 
				Neo4jQueryJobFactory.buildJobWithGenarateJob(h2Client, jedisPool, jsonMap);
		final ScheduledFuture<?> job = 
				scheduler.scheduleAtFixedRate(r, 0, interval, TimeUnit.SECONDS);
		
//		scheduler.schedule(new Runnable() {
//			public void run() {
//				job.cancel(true);
//			}
//		}, totalRunTime, TimeUnit.SECONDS);
	}
	
	private static void executeJobForPortForJsonFileName(
			String port, String jsonFileName, 
			ExecutorService executorService, H2Client h2Client) throws Exception {
		Map<String, Object> jsonMap = generateJobInDBFromJsonFileName(h2Client, jsonFileName);
		Runnable r					= Neo4jQueryJobFactory.buildJob(port, jsonMap);
		executorService.execute( r );
		logger.info("Job submitted to Neo-{}", port);
	}
	
	private static void executeJobForPortForJsonFileName(
			String jsonFileName, 
			ExecutorService executorService, H2Client h2Client) throws Exception {
		Map<String, Object> jsonMap = generateJobInDBFromJsonFileName(h2Client, jsonFileName);
		String port = jsonMap.get(JsonKeyConstants.START_PORT).toString();
		Runnable r	= Neo4jQueryJobFactory.buildJob(port, jsonMap);
		executorService.execute( r );
		logger.info("Job submitted to Neo-{}", port);
	}
	
	private static Map<String, Object> generateJobInDBFromJsonFileName(
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