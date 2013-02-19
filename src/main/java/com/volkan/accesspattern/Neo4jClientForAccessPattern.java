package com.volkan.accesspattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Neo4jClientForAccessPattern {

	private Map<Long, Long> nodeIDGidMap;
	
	public Neo4jClientForAccessPattern() {
		nodeIDGidMap = new HashMap<>();
	}
	
	protected Map<Long, List<Long>> collectNodeIDNeiIDsMap( GraphDatabaseService dbAP, 
															int maxNodeCount ) 
	{
		Map<Long, List<Long>> nodeIDNeiIDArrayMap = new HashMap<Long, List<Long>>();
		
		for (long i = 1; i <= maxNodeCount; i++) {
			Node node = dbAP.getNodeById(i);
			putToNodeIDGidMap(i, node);
			
			List<Long> neiIDs = new ArrayList<>();
			for (Relationship rel : node.getRelationships()) {
				Node otherNode = rel.getOtherNode(node);
				neiIDs.add(otherNode.getId());
			}
			nodeIDNeiIDArrayMap.put(node.getId(), neiIDs);
		}
		
		return nodeIDNeiIDArrayMap;
	}

	private void putToNodeIDGidMap(long i, Node node) {
		Object nullOrGid = node.getProperty("gid", null);
		if (nullOrGid != null) {
			nodeIDGidMap.put(i, new Long(((Integer) nullOrGid).longValue()));
		}
	}

	public Map<Long, Long> getNodeIDGidMap() {
		return nodeIDGidMap;
	}
}
