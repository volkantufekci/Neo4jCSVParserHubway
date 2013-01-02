package com.volkan.interpartitiontraverse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;

public abstract class AbstractTraverseHelper {

	public abstract List<String> traverse(GraphDatabaseService db, Map<String, Object> jsonMap);
	
	/**
	 * A shadow node came across within an unfinished path, 
	 * maybe at the beginning of a path or in the middle.
	 * 
	 * @param toDepth max depth of the specified query
	 * @param path current path for the traversal
	 * @param endNode of the path's current situation
	 * @return true means query should be delegated
	 */
	protected boolean didShadowComeInUnfinishedPath(int toDepth, Path path, Node endNode) {
		return path.length() < toDepth && ShadowEvaluator.isShadow(endNode);
	}
	
	
	/** 
	 * A Neo4j instance only knows the Gid and real partition's port of a node but
	 * local_id(neoid) of that node on the real partition of it. That's why all the 
	 * start_nodes coming from the JSON is first fetched from the index via its Gid
	 * by this method.
	 * 
	 * @param db
	 * @param jsonMap
	 * @return startNode
	 */
	protected Node fetchStartNodeFromIndex(GraphDatabaseService db, Map<String, Object> jsonMap) {
//		Node startNode = db.getNodeById((int) jsonMap.get("start_node"));
		IndexManager index = db.index();
		Index<Node> usersIndex = index.forNodes("users");
		IndexHits<Node> hits = usersIndex.get(PropertyNameConstants.GID, (int) jsonMap.get("start_node"));
		Node startNode = hits.getSingle();
		return startNode;
	}
	
	protected String appendEndingToFinishedPath( 
			Map<String, Object> jsonMap, Path path, Node endNode) {
	
		return path + " # " 
				+ endNode.getProperty(PropertyNameConstants.PORT, "NA") + "-"
				+ endNode.getProperty(PropertyNameConstants.GID) + " # "  
				+ "Hop count: " 
				+ jsonMap.get("hops");
}

	protected void increaseHops(Map<String, Object> jsonMap, Map<String, Object> jsonMapClone) {
		//Indicates how many additional hops performed in order to fulfill the query
		int hops = 0;
		if (jsonMap.containsKey("hops")) {
			hops = (int) jsonMap.get("hops");
		}
		hops++;
		jsonMapClone.put("hops", hops);
	}
	

	protected void updateRelationships(Path path, Map<String, Object> jsonMap,
			Map<String, Object> jsonMapClone) {
		@SuppressWarnings("unchecked")
		List<Map<String, String>> rels = (List<Map<String, String>>) jsonMap.get("relationships");
		int uptoDepth = path.length();
		jsonMapClone.put("relationships", pruneRelationships(rels, uptoDepth));
	}

	protected void updateDepth(Path path, Map<String, Object> jsonMap,
			Map<String, Object> jsonMapClone) {
		int uptoDepth = path.length();
		int newDepth  = (int)jsonMap.get(JsonKeyConstants.DEPTH) - uptoDepth;
		jsonMapClone.put(JsonKeyConstants.DEPTH, newDepth);
	}

	protected void updateStartNode(Path path, Map<String, Object> jsonMapClone) {
		Node endNode = path.endNode();
		jsonMapClone.put("start_node", new Integer((String)endNode.getProperty(PropertyNameConstants.GID)));
	}
	
	/**
	 * Prunes the given rels List. Removes the relations up to toDepth.
	 * 
	 * @param rels List to be pruned(not changed, a new one returns)
	 * @param toDepth rels up to this will be removed
	 * @return a new List of pruned relationships
	 */
	protected List<Map<String,String>> pruneRelationships(List<Map<String, String>> rels, int toDepth) {
		List<Map<String, String>> prunedRels = new ArrayList<>();
		
		for (int i = toDepth; i < rels.size(); i++) {
			prunedRels.add(rels.get(i));
		}
		
		return prunedRels;
	}
	
	protected String getPortFromEndNode(Path path) {
		Node endNode = path.endNode();
		String port = (String) endNode.getProperty(PropertyNameConstants.PORT);
		return port;
	}
}
