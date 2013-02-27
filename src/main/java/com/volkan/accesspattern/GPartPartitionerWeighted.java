package com.volkan.accesspattern;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.Configuration;

public class GPartPartitionerWeighted extends GPartPartitioner {

	private final static Logger logger = LoggerFactory.getLogger(GPartPartitionerWeighted.class);
	
	public static void buildWeightedGrfFile(Map<Long, List<EdgeWithWeight>> nodeIDEdgeArrayMap) 
			throws IOException
	{
		String content = generateWeightedGrfFileContent(nodeIDEdgeArrayMap);
		writeGrfFile(content);
	}
	
	protected static String generateWeightedGrfFileContent(Map<Long, List<EdgeWithWeight>> nodeIDEdgeArrayMap){
		
		StringBuilder sb = new StringBuilder();
		int nodeCount = 0;
		int relCount  = 0;
		TreeSet<Long> sortedNodeIDs = new TreeSet<>();
		
		sortedNodeIDs.addAll(nodeIDEdgeArrayMap.keySet());
		for (Long nodeID : sortedNodeIDs) {
//			SortedSet<Long> sortedNeis = new TreeSet<>();
//			sortedNeis.addAll(nodeIDEdgeArrayMap.get(nodeID));
//			
//			sb.append("\n" + sortedNeis.size());
//			for (Long neiNodeID : sortedNeis) {
//				sb.append("\t" + neiNodeID);
//			}
			
			List<EdgeWithWeight> edges = nodeIDEdgeArrayMap.get(nodeID);
			sb.append("\n"+edges.size());
			for (EdgeWithWeight eww : edges) {
				sb.append("\t"+eww.weight+"\t"+eww.otherNodeID);
			}
			
			nodeCount++;
//			relCount += sortedNeis.size();
			relCount += edges.size();
		}
		
		StringBuilder sbAna = new StringBuilder("0\n" + nodeCount + "\t" + relCount + "\n");
		sbAna.append("1\t010");
		sbAna.append(sb.toString());

		return sbAna.toString();
	}
	
	public static void performGpartingAndWriteGidPartitionMap(int partitionCount) 
				throws IOException, InterruptedException 
	{
		//SCOTCH
		partition(partitionCount);

		ConcurrentHashMap<Long, Integer> gidPartitionMap = readGpartResult();
		logger.info("gidPartitionMap.size()="+gidPartitionMap.size());
		writeGidPartitionMapForRuby(gidPartitionMap);
	}
	
	protected static ConcurrentHashMap<Long, Integer> readGpartResult() throws IOException {
		BufferedReader gpartResultFile = 
				new BufferedReader(new FileReader(Configuration.GPART_RESULT_PATH));
		int i = 0;
		String line = "";
		ConcurrentHashMap<Long, Integer> gidPartitionMap = new ConcurrentHashMap<Long, Integer>();
		while ( (line = gpartResultFile.readLine()) != null ) {
			if (i == 0){//skip the first line
				i++; continue;
			}
			
			String[] splitted = line.split("\t");
			Long nodeID       = new Long(splitted[0]);
			Integer partition = new Integer(splitted[1]);
			
			gidPartitionMap.put(nodeID, partition + 6474);//0 => 6474, 1 => 6475...
		}
		
		return gidPartitionMap;
	}
}
