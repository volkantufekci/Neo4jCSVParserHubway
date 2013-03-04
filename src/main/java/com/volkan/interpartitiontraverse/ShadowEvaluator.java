package com.volkan.interpartitiontraverse;

import java.util.ArrayList;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class ShadowEvaluator implements Evaluator{

	final ArrayList<VEdge> orderedPathContext;
	
	public ShadowEvaluator(ArrayList<VEdge> orderedPathContext) {
		this.orderedPathContext = orderedPathContext;
	}
	
	@Override
	public Evaluation evaluate(Path path) {
		if ( path.length() == 0 )
        {
            return Evaluation.EXCLUDE_AND_CONTINUE;
        }
		RelationshipType expectedType = orderedPathContext.get( path.length() - 1 ).getType();
        boolean isExpectedType = path.lastRelationship().isType( expectedType );
        
        Direction expectedDirection = orderedPathContext.get( path.length() - 1 ).getDirection();
        Relationship edge = path.lastRelationship();
        boolean isExpectedDirection = 
        		(expectedDirection.equals(Direction.OUTGOING)) ?
                	(edge.getEndNode().equals(path.endNode()))
                		: (edge.getStartNode().equals(path.endNode()));
        
        boolean included = isExpectedDirection && isExpectedType;
        boolean isReal = (boolean) path.endNode().getProperty(
        						PropertyNameConstants.SHADOW, true);
        boolean continued = isExpectedType && isReal;
        
        return Evaluation.of( included, continued );
		
//		if ( (boolean) path.endNode().getProperty(PropertyNameConstants.SHADOW, false) ) {
//			return Evaluation.INCLUDE_AND_PRUNE;
//		} else {
//			return Evaluation.INCLUDE_AND_CONTINUE;
//		}
	}
	
	public static boolean isShadow(Node endNode) {
		return (boolean)endNode.getProperty(PropertyNameConstants.SHADOW, false);
	}
}