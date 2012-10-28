package com.volkan.csv;

import java.util.Map;

public class NodeStructure {

	private NodePropertyHolder nodePropertyHolder;
	private Map<String,String> map;
	
	public NodeStructure(Map<String,String> map, NodePropertyHolder nodePropertyHolder) {
		setMap(map);
		setNodePropertyHolder(nodePropertyHolder);
	}

	public NodePropertyHolder getNodePropertyHolder() {
		return nodePropertyHolder;
	}
	public void setNodePropertyHolder(NodePropertyHolder nodePropertyHolder) {
		this.nodePropertyHolder = nodePropertyHolder;
	}

	public Map<String,String> getMap() {
		return map;
	}

	public void setMap(Map<String,String> map) {
		this.map = map;
	}
}
