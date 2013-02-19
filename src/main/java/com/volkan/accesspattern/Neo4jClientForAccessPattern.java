package com.volkan.accesspattern;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
															int maxNodeCount ) throws IOException 
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
		
		writeNodeIDGidMapForRuby();
		
		return nodeIDNeiIDArrayMap;
	}

	private void putToNodeIDGidMap(long i, Node node) {
		Object nullOrGid = node.getProperty("gid", null);
		if (nullOrGid != null) {
			nodeIDGidMap.put(i, new Long(((Integer) nullOrGid).longValue()));
		}
	}

	private void writeNodeIDGidMapForRuby() throws IOException {
		BufferedWriter gpartInputFile = new BufferedWriter(new FileWriter("gpartInputFile"));
		
		StringBuilder sb = new StringBuilder();
		for (Long nodeID : nodeIDGidMap.keySet()) {
			long gid = nodeIDGidMap.get(nodeID);
			sb.append(nodeID + "," + gid + "\n");
		}
		
		gpartInputFile.write( sb.toString() );
		
		if (gpartInputFile != null)
			gpartInputFile.close();
	}
	
	public Map<Long, Long> getNodeIDGidMap() {
		return nodeIDGidMap;
	}
}
