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

	/** Looks for nodes with gid property because other nodes are created in order to reference
	 * paths which do not exist in the original db. So this method skips path holding ref nodes.
	 * @param nodeID
	 * @param node
	 */
	private void putToNodeIDGidMap(long nodeID, Node node) {
		Object nullOrGid = node.getProperty("gid", null);
		if (nullOrGid != null) {
			nodeIDGidMap.put(nodeID, new Long(nullOrGid.toString()));
		}
	}

	private void writeNodeIDGidMapForRuby() throws IOException {
		BufferedWriter gpartInputFile = new BufferedWriter(new FileWriter("nodeIDGidMapForRuby"));
		
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
