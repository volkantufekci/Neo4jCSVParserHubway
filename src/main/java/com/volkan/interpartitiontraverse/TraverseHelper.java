package com.volkan.interpartitiontraverse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.TraversalDescription;

public class TraverseHelper {
	
	public List<String> traverse(GraphDatabaseService db, Map<String, Object> jsonMap) {
		List<String> realResults = new ArrayList<String>();
		TraversalDescription traversalDes = TraversalDescriptionBuilder.buildFromJsonMap(jsonMap);
		
		Node startNode = fetchStartNodeFromIndex(db, jsonMap);

		int toDepth = (Integer) jsonMap.get("depth");
		for (Path path : traversalDes.evaluator(new ShadowEvaluator())
									 .traverse(startNode)) {
			Node endNode = path.endNode();
			if (path.length() < toDepth && ShadowEvaluator.isShadow(endNode)) {
//				System.out.println("id: " + endNode.getId() + "\t" 
//										  + ShadowEvaluator.isShadow(endNode) + "\t"+path);
				List<String> delegatedResults = delegateQueryToAnotherNeo4j(path, jsonMap);
				for (String delegatedResult : delegatedResults) {
					realResults.add( path + " ~ " + delegatedResult );
				}
			} else {
				realResults.add( path + " # " + (String) endNode.getProperty("Port") + "-"
											  + (String) endNode.getProperty("Gid") );
			}
		}

		return realResults;
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
		IndexHits<Node> hits = usersIndex.get("Gid", (int) jsonMap.get("start_node"));
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
		jsonMapClone.put("start_node", new Integer((String)endNode.getProperty("Gid")));
		String port = (String) endNode.getProperty("Port");
		RestConnector restConnector = new RestConnector(port);
		String jsonString = restConnector.delegateQuery(jsonMapClone);
		List<String> resultList = convertJsonStringToList(jsonString);
		
		return resultList; 
	}

	private void increaseHops(Map<String, Object> jsonMap, Map<String, Object> jsonMapClone) {
		//Indicates how many additional hops performed in order to fulfill the query
		int hops = 0;
		if (jsonMap.containsKey("hops")) {
			hops = (int) jsonMap.get("hops");
		}
		hops++;
		jsonMapClone.put("hops", hops);
	}

	private List<String> convertJsonStringToList(String jsonString) {
		ObjectMapper mapper = new ObjectMapper();
		List<String> resultList = new ArrayList<>();
		try {
			@SuppressWarnings("unchecked")
			List<String> jsonList = mapper.readValue(jsonString, List.class);
			resultList.addAll(jsonList);
		} catch (IOException e) {
			resultList.add("jsonString could not be read");
		}
		return resultList;
	}

//	public class ShadowEvaluator implements Evaluator{
//
//		@Override
//		public Evaluation evaluate(Path path) {
//			if (isShadow(path.endNode())) {
//				return Evaluation.INCLUDE_AND_PRUNE;
//			} else {
//				return Evaluation.INCLUDE_AND_CONTINUE;
//			}
//		}
//		
//	}
//
//	private boolean isShadow(Node endNode) {
//		return (boolean)endNode.getProperty("Shadow");
//	}

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
