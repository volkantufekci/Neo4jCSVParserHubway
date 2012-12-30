package com.volkan.interpartitiontraverse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.util.StringLogger;

import com.volkan.Utility;

public class TraverseHelper {
	
	private final StringLogger logger = StringLogger.logger(Utility.buildLogFileName());
	
	public List<String> traverse(GraphDatabaseService db, Map<String, Object> jsonMap) {
		List<String> realResults = new ArrayList<String>();
		TraversalDescription traversalDes = TraversalDescriptionBuilder.buildFromJsonMap(jsonMap);
		
//		8474'e(unpartitioned) ozgu olarak index yerine dogrudan id'den alinmali start_node
//		cunku 8474'te index yok!
//		Node startNode = db.getNodeById((int) jsonMap.get("start_node"));
		Node startNode = fetchStartNodeFromIndex(db, jsonMap);

		int toDepth = (Integer) jsonMap.get("depth");
		for (Path path : traversalDes.traverse(startNode)) {
			logger.logMessage(path.toString() + " # " + path.length(), true);
			Node endNode = path.endNode();
			if (didShadowComeInUnfinishedPath(toDepth, path, endNode)) {
				List<String> delegatedResults = delegateQueryToAnotherNeo4j(path, jsonMap);
				realResults.addAll( appendDelegatedResultsToPath( path, delegatedResults ));
			} else {
				if (path.length() >= toDepth) { //if it is a finished path
					realResults.add( appendEndingToFinishedPath(jsonMap, path, endNode) );
				} //else, a real node but unfinished path. No need to care
			}
		}

		return realResults;
	}
	
	/**
	 * A shadow node came across within an unfinished path, 
	 * maybe at the beginning of a path or in the middle.
	 * 
	 * @param toDepth max depth of the specified query
	 * @param path current path for the traversal
	 * @param endNode of the path's current situation
	 * @return true means query should be delegated
	 */
	private boolean didShadowComeInUnfinishedPath(int toDepth, Path path,
			Node endNode) {
		return path.length() < toDepth && ShadowEvaluator.isShadow(endNode);
	}

	private List<String> appendDelegatedResultsToPath(Path path, List<String> delegatedResults) {
		Node endNode = path.endNode();
		String port = (String) endNode.getProperty(PropertyNameConstants.PORT);
		List<String> results = new ArrayList<>();
		for (String delegatedResult : delegatedResults) {
			results.add( path + "~{" + port + "}" + delegatedResult );
		}
		
		return results;
	}
	
	private String appendEndingToFinishedPath( 
				Map<String, Object> jsonMap, Path path, Node endNode) {
		
		return path + " # " 
					+ endNode.getProperty(PropertyNameConstants.PORT, "NA") + "-"
					+ endNode.getProperty(PropertyNameConstants.GID) + " # "  
					+ "Hop count: " 
					+ jsonMap.get("hops");
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
	private Node fetchStartNodeFromIndex(GraphDatabaseService db, Map<String, Object> jsonMap) {
//		Node startNode = db.getNodeById((int) jsonMap.get("start_node"));
		IndexManager index = db.index();
		Index<Node> usersIndex = index.forNodes("users");
		IndexHits<Node> hits = usersIndex.get(PropertyNameConstants.GID, (int) jsonMap.get("start_node"));
		Node startNode = hits.getSingle();
		return startNode;
	}

	private List<String> delegateQueryToAnotherNeo4j(Path path, Map<String, Object> jsonMap) {
		Map<String, Object> jsonMapClone = new HashMap<String, Object>();
		@SuppressWarnings("unchecked")
		List<Map<String, String>> rels = (List<Map<String, String>>) jsonMap.get("relationships");
		int uptoDepth = path.length();
		jsonMapClone.put("relationships", pruneRelationships(rels, uptoDepth)); 
		
		increaseHops(jsonMap, jsonMapClone);
		
		int newDepth = (int)jsonMap.get("depth") - uptoDepth;
		jsonMapClone.put("depth", newDepth);
		
		Node endNode = path.endNode();
		jsonMapClone.put("start_node", new Integer((String)endNode.getProperty(PropertyNameConstants.GID)));
		String port = (String) endNode.getProperty(PropertyNameConstants.PORT);
		RestConnector restConnector = new RestConnector(port);
		String jsonString = restConnector.delegateQuery(jsonMapClone);
		List<String> resultList = JsonHelper.convertJsonStringToList(jsonString);
		
		return resultList; 
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

	/**
	 * Prunes the given rels List. Removes the relations up to toDepth.
	 * 
	 * @param rels List to be pruned(not changed, a new one returns)
	 * @param toDepth rels up to this will be removed
	 * @return a new List of pruned relationships
	 */
	public List<Map<String,String>> pruneRelationships(List<Map<String, String>> rels, int toDepth) {
		List<Map<String, String>> prunedRels = new ArrayList<>();
		
		for (int i = toDepth; i < rels.size(); i++) {
			prunedRels.add(rels.get(i));
		}
		
		return prunedRels;
	}
}
