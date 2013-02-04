package com.volkan;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.volkan.csv.NodePropertyHolder;
import com.volkan.csv.StationNodePropertyHolder;
import com.volkan.interpartitiontraverse.PropertyNameConstants;

public class Neo4jClient {

	private static final Logger logger = LoggerFactory.getLogger(Neo4jClient.class);

	class FollowerCountHolder implements Comparable<FollowerCountHolder>{
		public int nodeID;
		public int followerCount;
		
		FollowerCountHolder(int nodeID, int followerCount){
			this.nodeID = nodeID;
			this.followerCount = followerCount;
		}
		@Override
		public int compareTo(FollowerCountHolder o) {
			if (this.followerCount > o.followerCount) {
				return -1;
			} else if (this.followerCount < o.followerCount){
				return 1;
			}
			return 0;
		}
	}
	
	/**
	 * @param db
	 * @param maxNodeID 1850065 for erdos
	 * @param relName "follows" for erdos
	 * @param direction Direction.INCOMING for erdos
	 */
	public void getMostFollowedNodes(GraphDatabaseService db, int maxNodeID, 
										String relName, Direction direction) {
		List<FollowerCountHolder> list = new ArrayList<FollowerCountHolder>(10);
		int limit = 10;
		for (int i = 0; i < limit; i++) {
			FollowerCountHolder fch = new FollowerCountHolder(i, 0);
			list.add(fch);
		}
		
		int smallestCount = 0;
		
		for (int nodeID = 1; nodeID <= maxNodeID; nodeID++) {
			Node startNode = db.getNodeById(nodeID);
			if ( (boolean) startNode.getProperty(PropertyNameConstants.SHADOW, false)) {
				continue;
			}
			TraversalDescription td = Traversal.description();
			td = td.depthFirst()
					.relationships(DynamicRelationshipType.withName(relName), direction)
					.evaluator(Evaluators.atDepth(1));
			int followerCount = 0;
			for (Path path : td.traverse(startNode)) {
				logger.debug("{}",path.toString());
				followerCount++;
			}
			
			boolean isSmallerFound = false;
			if (followerCount > smallestCount){
				for (FollowerCountHolder fch : Lists.reverse(list)) {
					if (followerCount >= fch.followerCount) {
						isSmallerFound = true;
						break;
					}
				}
				
				if (isSmallerFound) {
					list.remove(list.size() - 1);//remove last one
					list.add(new FollowerCountHolder(nodeID, followerCount));
					Collections.sort(list);//sort again as new fch added
					FollowerCountHolder temp = list.get(list.size() - 1);//take the last one
					smallestCount = temp.followerCount;
				}
			}
			
			logger.info("NodeID = {}",nodeID);
		}
		
		for (FollowerCountHolder followerCountHolder : list) {
			logger.info("NodeID={} FollowerCount={}", 
							followerCountHolder.nodeID, followerCountHolder.followerCount);
		}
	}

	public void getMostFollowedErdos(GraphDatabaseService db, int maxNodeID, 
			String relName, Direction direction) {
		getMostFollowedNodes(db, maxNodeID, relName, direction);
	}

	
	public static void myFriendDepth3(GraphDatabaseService db) {
		Node startNode = db.getNodeById(1);
		TraversalDescription td = Traversal.description();
		td = td.depthFirst()
				.relationships(DynamicRelationshipType.withName("following"), Direction.OUTGOING)
				.evaluator(Evaluators.atDepth(5));
		int count = 0;
//		Set<Long> set = new HashSet<Long>();
		for (Path path : td.traverse(startNode)) {
//			logger.info(node.toString());
			logger.info("{}",path.toString());
			count++;
//			set.add(node.getId());
		}
		logger.info("Count = {}", count);
	}
	
	public static void getNodesWithMostFriendsFromEgullerTwitterDB(ExecutionEngine engine) {
		ExecutionResult result = engine.execute(
									"START friend=node(*)" +
									"MATCH friend<-[:following]-()" + 
									"RETURN friend, friend.twitter_id, count(*)" +     
									"ORDER BY count(*) DESC LIMIT 15");
		StringBuilder rows = null;
		for (Map<String, Object> row : result) {
			rows = new StringBuilder();
			for (Entry<String, Object> column : row.entrySet()) {
				rows.append(column.getKey()+ ": " +column.getValue()+ " | ");
			}
			logger.info(rows.toString());
		}
	}
	
	public static void testMostUsedStations(ExecutionEngine engine) {
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
	
	public static void getMostUsedStationsConnections(ExecutionEngine engine) {
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

	private static Map<Long, Integer> assignPartitionsToCypherResult(ExecutionResult result) {
		Map<Long, Integer> hmGidPartition = new HashMap<Long, Integer>();

		for (Map<String, Object> row : result) {
			for (Entry<String, Object> column : row.entrySet()) {
				hmGidPartition.put((Long) column.getValue(), 1);
			}
		}

		return hmGidPartition;
	}

	private static void assignPartitionToRemainingNodes(Map<Long, Integer> hmGidPartition) {
		for (long i = 1; i <= Configuration.MAX_NODE_COUNT; i++) {
			if (!hmGidPartition.containsKey(i)) {
				hmGidPartition.put(i, 0);
			}
		}
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

	private static void writeToFile(String content, String fileName) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
			out.write(content);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	

	public static void test10RelFetch(ExecutionEngine engine) {
		StringBuilder sb = new StringBuilder();
		for (int i = 1000; i < 1010; i++) {
			sb.append(i + ",");
		}
		sb.deleteCharAt(sb.length() - 1); // remove last ","

		ExecutionResult result = engine.execute("START stat=relationship("
				+ sb.toString() + ")" + "RETURN stat;");

		logger.info(result.toString());

		for (Map<String, Object> row : result) {
			Relationship rel = (Relationship) row.values().iterator().next();

			Node startNode = rel.getStartNode();
			Node endNode = rel.getEndNode();

			logger.info(startNode.getId() + "-[:" + rel.getType()
					+ "]->" + endNode.getId());
		}
	}

	public static void test10NodeFetch(ExecutionEngine engine) {
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

	public static void testPrintingOrdersOfColumns(ExecutionEngine engine) {
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
	
}
