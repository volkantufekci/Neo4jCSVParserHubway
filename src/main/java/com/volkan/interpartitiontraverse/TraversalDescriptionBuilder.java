package com.volkan.interpartitiontraverse;

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
		builder.addRelationships(jsonMap);
		builder.addShadowEvaluator();
		builder.addUniqueness();
		return builder.build();
	}

	private TraversalDescriptionBuilder() {
		traversalDescription = Traversal.description();
	}
	
	private void addDepth(Map<String, Object> jsonMap) {
		int depth = (Integer) jsonMap.get("depth");
		traversalDescription = 
				traversalDescription.evaluator(Evaluators.fromDepth(1))
									.evaluator(Evaluators.toDepth(depth));
	}

	@SuppressWarnings("unchecked")
	private void addRelationships(Map<String, Object> jsonMap) {
		for (Map<String, String> rel : (List<Map<String, String>>) jsonMap.get("relationships")) {
			String relationName = rel.get("type");
			Direction direction = findOutDirection(rel);
			traversalDescription = 
					traversalDescription.relationships(
							DynamicRelationshipType.withName(relationName), direction);
		}
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
	
	private void addShadowEvaluator() {
		traversalDescription = traversalDescription.evaluator(new ShadowEvaluator());
	}
	
	private void addUniqueness() {
		traversalDescription = traversalDescription.uniqueness(Uniqueness.NONE);
	}
	
	public TraversalDescription build() {
		return traversalDescription;
	}

}
