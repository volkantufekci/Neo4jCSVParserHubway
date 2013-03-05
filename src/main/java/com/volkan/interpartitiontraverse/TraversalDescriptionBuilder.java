package com.volkan.interpartitiontraverse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class TraversalDescriptionBuilder {
	private TraversalDescription traversalDescription;
	
	public static TraversalDescription buildFromJsonMap(Map<String, Object> jsonMap) {
		TraversalDescriptionBuilder builder = new TraversalDescriptionBuilder();
		builder.addDepth(jsonMap);
		builder.addShadowEvaluator(jsonMap);
		builder.addUniqueness();
		return builder.build();
	}

	private TraversalDescriptionBuilder() {
		traversalDescription = Traversal.description();
	}
	
	private void addDepth(Map<String, Object> jsonMap) {
		int depth = (Integer) jsonMap.get(JsonKeyConstants.DEPTH);
		traversalDescription = 
				traversalDescription.evaluator(Evaluators.fromDepth(1))
									.evaluator(Evaluators.toDepth(depth));
	}
	
	private Direction findOutDirection(Map<String, String> rel) {
		String directionString = rel.get("direction");
		Direction direction = null;
		if (directionString.equalsIgnoreCase("IN")) {
			direction = Direction.INCOMING;
		} else {
			direction = Direction.OUTGOING;
		}
		return direction;
	}
	
	@SuppressWarnings("unchecked")
	private void addShadowEvaluator(Map<String, Object> jsonMap) {
		ArrayList<VEdge> orderedPathContext = new ArrayList<>();
		
		for (Map<String, String> rel : (List<Map<String, String>>) jsonMap.get("relationships")) {
			String relationName = rel.get("type");
			Direction direction = findOutDirection(rel);
			VEdge vedge = new VEdge(direction, DynamicRelationshipType.withName(relationName));
			orderedPathContext.add(vedge);
		}
		
		traversalDescription = 
				traversalDescription.evaluator(new ShadowEvaluator(orderedPathContext));
	}
	
	private void addUniqueness() {
		traversalDescription = traversalDescription.uniqueness(Uniqueness.NODE_PATH);
	}
	
	public TraversalDescription build() {
		return traversalDescription;
	}

}
