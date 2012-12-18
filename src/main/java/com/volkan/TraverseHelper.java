package com.volkan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

public class TraverseHelper {
	
	public List<String> traverse(GraphDatabaseService db, Map<String, Object> jsonMap) {
		List<String> realResults = new ArrayList<String>();
		int toDepth = (Integer) jsonMap.get("depth");
		TraversalDescription traversalDesc = buildTraversalDescFromJsonMap(jsonMap);
		
//		Node startNode = db.getNodeById((int) jsonMap.get("start_node"));
		Node startNode = fetchStartNodeFromIndex(db, jsonMap);
		
		for (Path path : traversalDesc.evaluator(new ShadowEvaluator())
									  .traverse(startNode)) {
			Node endNode = path.endNode();
			if (path.length() < toDepth && isShadow(endNode)) {
				System.out.println("id: " + endNode.getId() + "\t" + isShadow(endNode) + "\t"+path);
				String delegatedResults = delegateQueryToAnotherNeo4j(path, jsonMap);
				realResults.add(delegatedResults);
			} else {
				realResults.add((String) endNode.getProperty("Port")
								+ "-"
								+ (String) endNode.getProperty("Gid")
								+ "\n");
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
		IndexManager index = db.index();
		Index<Node> usersIndex = index.forNodes("users");
		IndexHits<Node> hits = usersIndex.get("Gid", (int) jsonMap.get("start_node"));
		Node startNode = hits.getSingle();
		return startNode;
	}

	private TraversalDescription buildTraversalDescFromJsonMap(Map<String, Object> jsonMap) {
		TraversalDescription traversalDesc;
		traversalDesc = Traversal.description();
		traversalDesc = addDepth(jsonMap, traversalDesc);
		traversalDesc = addRelationships(jsonMap, traversalDesc);
		return traversalDesc;
	}

	private String delegateQueryToAnotherNeo4j(Path path, Map<String, Object> jsonMap) {
		Map<String, Object> jsonMapClone = new HashMap<String, Object>();
		@SuppressWarnings("unchecked")
		List<Map<String, String>> rels = (List<Map<String, String>>) jsonMap.get("relationships");
		int uptoDepth = path.length();
		jsonMapClone.put("relationships", pruneRelationships(rels, uptoDepth)); 
		
		int newDepth = (int)jsonMap.get("depth") - uptoDepth;
		jsonMapClone.put("depth", newDepth);
		
		Node endNode = path.endNode();
		jsonMapClone.put("start_node", new Integer((String)endNode.getProperty("Gid")));
		String port = (String) endNode.getProperty("Port");
		RestConnector restConnector = new RestConnector(port);
		return restConnector.delegateQuery(jsonMapClone);
	}

	TraversalDescription addDepth(Map<String, Object> jsonMap, TraversalDescription traversal) {
		int depth = (Integer) jsonMap.get("depth");
		return traversal.evaluator(Evaluators.fromDepth(1))
						.evaluator(Evaluators.toDepth(depth));
	}

	@SuppressWarnings("unchecked")
	TraversalDescription addRelationships(Map<String, Object> jsonMap,
													TraversalDescription traversal) {
		
		for (Map<String, String> rel : (List<Map<String, String>>) jsonMap.get("relationships")) {
			String relationName = rel.get("type");
			Direction direction = findOutDirection(rel);
			traversal = traversal.relationships(
					DynamicRelationshipType.withName(relationName), direction);
		}
		return traversal;
	}

	Direction findOutDirection(Map<String, String> rel) {
		String directionString = rel.get("direction");
		Direction direction = null;
		if (directionString.equalsIgnoreCase("IN")) {
			direction = Direction.INCOMING;
		} else {
			direction = Direction.OUTGOING;
		}
		return direction;
	}
	
	public class ShadowEvaluator implements Evaluator{

		@Override
		public Evaluation evaluate(Path path) {
			if (isShadow(path.endNode())) {
				return Evaluation.INCLUDE_AND_PRUNE;
			} else {
				return Evaluation.INCLUDE_AND_CONTINUE;
			}
		}
		
	}

	private boolean isShadow(Node endNode) {
		return (boolean)endNode.getProperty("Shadow");
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
