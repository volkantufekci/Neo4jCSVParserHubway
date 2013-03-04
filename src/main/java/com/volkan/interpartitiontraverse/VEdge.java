package com.volkan.interpartitiontraverse;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

public class VEdge {

	Direction direction;
	RelationshipType type;
	
	public VEdge(Direction direction, RelationshipType type) {
		this.direction = direction;
		this.type      = type;
	}

	public Direction getDirection() {
		return direction;
	}

	public RelationshipType getType() {
		return type;
	}

	public void setDirection(Direction direction) {
		this.direction = direction;
	}

	public void setType(RelationshipType type) {
		this.type = type;
	}
}
