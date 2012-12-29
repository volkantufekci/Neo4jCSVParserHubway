package com.volkan.interpartitiontraverse;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class ShadowEvaluator implements Evaluator{

	@Override
	public Evaluation evaluate(Path path) {
		if ( (boolean) path.endNode().getProperty(PropertyNameConstants.SHADOW, false) ) {
			return Evaluation.INCLUDE_AND_PRUNE;
		} else {
			return Evaluation.INCLUDE_AND_CONTINUE;
		}
	}
	
	public static boolean isShadow(Node endNode) {
		return (boolean)endNode.getProperty(PropertyNameConstants.SHADOW, false);
	}
}