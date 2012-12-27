package com.volkan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.csv.NodePropertyHolder;
import com.volkan.csv.StationNodePropertyHolder;
import com.volkan.interpartitiontraverse.JsonHelper;
import com.volkan.interpartitiontraverse.RestConnector;
import com.volkan.interpartitiontraverse.TraverseHelper;

public class Main {

	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	private static final String DB_PATH = "./src/main/resources/graph.db";
	private static GraphDatabaseService db;
	
	public static void main(String[] args) {

		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook();

		ExecutionEngine engine = new ExecutionEngine(db);
		int hede = 7;
		switch (hede) {
		case 0:
			test10NodeFetch(engine);
			break;
		case 1:
			testMostUsedStations(engine);
			break;
		case 2:
			testPrintingOrdersOfColumns(engine);
			break;
		case 3:
			test10RelFetch(engine);
			break;
		case 4:
			getMostUsedStationsConnections(engine);
			break;
		case 5:
			traverseWithShadowEvaluator();
			break;
		case 6:
			traverseViaJsonMap(readJsonFileIntoMap("testhop.json"));
			break;
		case 7:
			String port = "7474";
			String url	= "http://localhost";
			delegateQueryToAnotherNeo4j(url, port, readJsonFileIntoMap("testhop.json"));
			break;
		case 8:
			indexFetchDeneme();
			break;
		default:
			break;
		}
	}

	private static void indexFetchDeneme() {
		IndexManager index = db.index();
		Index<Node> usersIndex = index.forNodes("users");
		IndexHits<Node> hits = usersIndex.get("Gid", 1904);
		Node node = hits.getSingle();
		System.out.println(node);
	}

	@SuppressWarnings("unchecked")
	protected static Map<String, Object> readJsonFileIntoMap(String fileName) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> jsonMap	= null;
		try {
			jsonMap = mapper.readValue(new File(fileName), Map.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return jsonMap;
	}
	
	private static void delegateQueryToAnotherNeo4j(String url, String port, 
													Map<String, Object> jsonMap) {	
		RestConnector restConnector = new RestConnector(url, port);
		String jsonString = restConnector.delegateQuery(jsonMap);
		List<String> resultList = JsonHelper.convertJsonStringToList(jsonString);
		for (String result : resultList) {
			logger.info(result);
		}
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
				.evaluator(new Main.ShadowEvaluator())
				.traverse(node)) {
				Node endNode = path.endNode();
				if (path.length() < toDepth) {
//					String gid = (String) endNode.getProperty("Gid");
					shadowResults.add(endNode);
					System.out.println("id: " + endNode.getId() + "\t" + isShadow(endNode) + "\t" + path );
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
	
	public static class ShadowEvaluator implements Evaluator{

		@Override
		public Evaluation evaluate(Path path) {
			if (isShadow(path.endNode())) {
				return Evaluation.INCLUDE_AND_PRUNE;
			} else {
				return Evaluation.INCLUDE_AND_CONTINUE;
			}
		}
		
	}

	private static boolean isShadow(Node endNode) {
		return (boolean)endNode.getProperty("Shadow");
	}

	private static void getMostUsedStationsConnections(ExecutionEngine engine) {
		ExecutionResult result = engine.execute( 
				  "START stat=node(12) "
				+ "MATCH stat--other "
				+ "RETURN ID(other)" );
		
		Map<Long, Integer> hmGidPartition = assignPartitionsToCypherResult(result);
		hmGidPartition.put(12l, 1); //The node(station) we queried should also be added.
		
		assignPartitionToRemainingNodes(hmGidPartition);
		
		String sortedLines = buildSortedLines(hmGidPartition);

		writeToFile(sortedLines, System.getProperty("user.home") + "/gid_partition_h");
	}

	private static String buildSortedLines(Map<Long, Integer> hmGidPartition) {
		List<Long> gids = new ArrayList<Long>();
		gids.addAll(hmGidPartition.keySet());
		Collections.sort(gids);

		StringBuilder sb = new StringBuilder();
		for (Long gid : gids) {
			sb.append(gid + "," + hmGidPartition.get(gid) + "\n");
		}
		return sb.toString();
	}

	private static void assignPartitionToRemainingNodes(Map<Long, Integer> hmGidPartition) {
		for (long i = 1; i <= Configuration.MAX_NODE_COUNT; i++) {
			if (!hmGidPartition.containsKey(i)) {
				hmGidPartition.put(i, 0);
			}
		}
	}

	private static Map<Long, Integer> assignPartitionsToCypherResult(ExecutionResult result) {
		Map<Long, Integer> hmGidPartition = new HashMap<Long, Integer>();

		for (Map<String, Object> row : result) {
			for (Entry<String, Object> column : row.entrySet()) {
				hmGidPartition.put((Long) column.getValue(), 1);
			}
		}

		return hmGidPartition;
	}

	private static void testMostUsedStations(ExecutionEngine engine) {
		ExecutionResult result = engine
				.execute("START stat=node:Station(\"stationId:*\") "
						+ " MATCH stat<-[:`START`]-()-[:END]->endstat "
						+ " RETURN stat.name,endstat.name,count(*)"
						+ " ORDER BY count(*) DESC LIMIT 15;");

		System.out.println(result);

		String rows = "";
		for (Map<String, Object> row : result) {
			for (Entry<String, Object> column : row.entrySet()) {
				rows += column.getKey() + ": " + column.getValue() + " | ";
			}
			rows += "\n";
		}
		System.out.println(rows);
	}

	private static void test10RelFetch(ExecutionEngine engine) {
		StringBuilder sb = new StringBuilder();
		for (int i = 1000; i < 1010; i++) {
			sb.append(i + ",");
		}
		sb.deleteCharAt(sb.length() - 1); // remove last ","

		ExecutionResult result = engine.execute("START stat=relationship("
				+ sb.toString() + ")" + "RETURN stat;");

		System.out.println(result);

		for (Map<String, Object> row : result) {
			Relationship rel = (Relationship) row.values().iterator().next();

			Node startNode = rel.getStartNode();
			Node endNode = rel.getEndNode();

			System.out.println(startNode.getId() + "-[:" + rel.getType()
					+ "]->" + endNode.getId());
		}
	}

	private static void test10NodeFetch(ExecutionEngine engine) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 10; i++) {
			sb.append(i + ",");
		}
		sb.deleteCharAt(sb.length() - 1); // remove last ","

		ExecutionResult result = engine.execute("START stat=node("
				+ sb.toString() + ")" + "RETURN stat;");

		System.out.println(result);

		for (Map<String, Object> row : result) {
			Node node = (Node) row.values().iterator().next();

			// NodePropertyHolder stationNodePropertyHolder = new
			// StationNodePropertyHolder();
			// for (String prop :
			// stationNodePropertyHolder.getNodePropertyNames()) {
			// System.out.println(prop + " => " + node.getProperty(prop));
			// }

			for (String prop : node.getPropertyKeys()) {
				System.out.println(prop + " => " + node.getProperty(prop));
			}
			System.out.println();
		}
	}

	private static void testPrintingOrdersOfColumns(ExecutionEngine engine) {
		ExecutionResult result = engine
				.execute("START stat=node:Station(\"stationId:*\") "
						+ "RETURN stat LIMIT 15;");

		System.out.println(result);

		for (Map<String, Object> row : result) {
			Node node = (Node) row.values().iterator().next();

			NodePropertyHolder stationNodePropertyHolder = new StationNodePropertyHolder();
			for (String prop : stationNodePropertyHolder.getNodePropertyNames()) {
				System.out.println(prop + " => " + node.getProperty(prop));
			}

			// for (String prop : node.getPropertyKeys()) {
			// System.out.println(prop + " => " + node.getProperty(prop));
			// }
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

	private static void writeToFile(String content, String fileName) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
			out.write(content);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}