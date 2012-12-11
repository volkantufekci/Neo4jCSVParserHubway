package com.volkan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.OrderedByTypeExpander;
import org.neo4j.kernel.Traversal;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.volkan.csv.NodePropertyHolder;
import com.volkan.csv.StationNodePropertyHolder;

public class Main {

	private static final String DB_PATH = "./src/main/resources/graph.db";

	private static GraphDatabaseService db;

	public static void main(String[] args) {

		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook();

		// add some data first
		Transaction tx = db.beginTx();
		try {
			Node refNode = db.getReferenceNode();
			refNode.setProperty("name", "reference node");
			tx.success();
		} finally {
			tx.finish();
		}

		ExecutionEngine engine = new ExecutionEngine(db);
		int hede = 5;
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
			traversalDeneme();
			break;
		case 6:
			jacksonDeneme();
			break;
		case 7:
			jsonClient();
			break;
		default:
			break;
		}
	}
	
	private static void jsonClient() {
		Client client = Client.create();
		WebResource webResource = client.resource("http://localhost:8474/example/service/volkan");
		ObjectMapper mapper = new ObjectMapper();
		
		try {
			Map<String,Object> jsonMap = mapper.readValue(new File("user-modified.json"), Map.class);
			ClientResponse clientResponse = 
					webResource.type("application/json")
							   .post(ClientResponse.class, mapper.writeValueAsString(jsonMap));
			
			System.out.println(clientResponse.getEntity(String.class));
		} catch (UniformInterfaceException | ClientHandlerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void jacksonDeneme() {
		ObjectMapper mapper = new ObjectMapper();
		try {
//			mapper.writeValue(new File("user-modified.json"), userData);
			Map<String,Object> jsonMap = mapper.readValue(new File("user-modified.json"), Map.class);
			int depth = (Integer) jsonMap.get("depth");
			TraversalDescription traversal = Traversal.description().evaluator(Evaluators.atDepth(depth));
			
			for (Map<String, String> rel : (List<Map<String, String>>)jsonMap.get("relationships")) {
				String relationName = rel.get("type");
				Direction direction = findOutDirection(rel);
				traversal = traversal.relationships(DynamicRelationshipType.withName(relationName), direction);
			}
			
			Node startNode = db.getNodeById((int)jsonMap.get("start_node"));
			
			List<String> nodes = new ArrayList<String>();
			for (Node node : traversal.traverse(startNode).nodes()) {
				String name = (String) node.getProperty("name");
				System.out.println(name);
				nodes.add(name);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static Map<String, Object> buildJsonMap() {
		Map<String,Object> userData = new HashMap<String,Object>();
		Map<String,String> rel1 = new HashMap<String,String>();
		rel1.put("type", "END");
		rel1.put("direction", "IN");
		
		Map<String,String> rel2 = new HashMap<String,String>();
		rel2.put("type", "BIKE");
		rel2.put("direction", "OUT");

		List<Map<String, String>> relationships = new ArrayList<Map<String, String>>();
		relationships.add(rel1); 
		relationships.add(rel2); 
		
		userData.put("relationships", relationships);
		userData.put("client", "NEO4J");
		userData.put("depth", 2);
		userData.put("start_node", 12);
		return userData;
	}
	
	private static Direction findOutDirection(Map<String, String> rel) {
		String directionString	= rel.get("direction");
		Direction direction = null;
		if (directionString.equalsIgnoreCase("IN")) {
			direction = Direction.INCOMING;
		} else {
			direction = Direction.OUTGOING;
		}
		return direction;
	}

	
	private static void traversalDeneme() {
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
//				if (path.length() < toDepth && isShadow(endNode)) {
//					String gid = (String) endNode.getProperty("Gid");
					System.out.println("id: " + endNode.getId() + "\t" + isShadow(endNode) + "\t" + path );
//				}
			i++;		
		}
		System.out.println(i);
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