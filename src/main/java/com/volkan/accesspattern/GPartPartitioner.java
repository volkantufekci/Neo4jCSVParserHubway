package com.volkan.accesspattern;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class GPartPartitioner {

	public static void buildGrfFile(Map<Integer, List<Integer>> nodeIDNeiIDArrayMap) throws IOException{
		String content = generateGrfFileContent(nodeIDNeiIDArrayMap);
		writeGrfFile(content);
	}
	
	protected static String generateGrfFileContent(Map<Integer, List<Integer>> nodeIDNeiIDArrayMap){
		
		StringBuilder sb = new StringBuilder();
		int nodeCount = 0;
		int relCount  = 0;
		
		for (Integer nodeID : nodeIDNeiIDArrayMap.keySet()) {
			SortedSet<Integer> sortedNeis = new TreeSet<>();
			sortedNeis.addAll(nodeIDNeiIDArrayMap.get(nodeID));
			
			sb.append("\n" + sortedNeis.size());
			for (Integer neiNodeID : sortedNeis) {
				sb.append("\t" + neiNodeID);
			}
			
			nodeCount++;
			relCount += sortedNeis.size();
		}
		
		StringBuilder sbAna = new StringBuilder("0\n" + nodeCount + "\t" + relCount + "\n");
		sbAna.append("1\t000");
		sbAna.append(sb.toString());

		return sbAna.toString();
	}
	
	protected static void writeGrfFile(String content) throws IOException {

		BufferedWriter gpartInputFile = new BufferedWriter(new FileWriter("gpartInputFile"));
		
		gpartInputFile.write( content );
		
		if (gpartInputFile != null)
			gpartInputFile.close();
	}
	
	
}
