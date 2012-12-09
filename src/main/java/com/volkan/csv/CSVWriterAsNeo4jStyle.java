package com.volkan.csv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.volkan.Configuration;

public class CSVWriterAsNeo4jStyle {

	public void appendToRelsCSV(String startGid, String endGid, String relType, int relId) {
		String content = startGid + "\t" + endGid + "\t" + relType + "\t" + relId + "\n";
		appendToFile(content, Configuration.RELS_CSV);		
	}
	
	public void appendToNodesCSV(String content) {
		appendToFile(content, Configuration.NODES_CSV);
	}

	private void appendToFile(String content, String fileName) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(fileName, true));
		    out.write(content);
		    out.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	}
	
	public void writeHeadersOfNodesCSV(String headers) {
		writeToFile(headers, Configuration.NODES_CSV);
	}
	
	public void writeHeadersOfRelsCSV() {
		String headers = "Start\tEnde\tType\tRelId\n";
		writeToFile(headers, Configuration.RELS_CSV);
	}

	private void writeToFile(String content, String fileName) {
		try {
		    BufferedWriter out = new BufferedWriter(new FileWriter(fileName));
		    out.write(content);
		    out.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
	}	
	
	public String tabSeparate(List<NodeStructure> nodeStructures, 
								CSVColumnPositioner colPositioner) {

		sortByGidAsc(nodeStructures);
		
		StringBuilder sb = new StringBuilder();

		for (NodeStructure nodeStructure : nodeStructures) {
			NodePropertyHolder nph 				= nodeStructure.getNodePropertyHolder();
			String[] properties 				= nph.getNodePropertyNames();
			Map<String, Integer> hmColumnOrder 	= colPositioner.columnOrdersFor(nph);

			Map<String, String> node = nodeStructure.getMap();

			int position = 0;
			for (String property : properties) {
				int columnOrder = hmColumnOrder.get(property);
				int tabCount 	= columnOrder - position;
				for (int i = 0; i < tabCount; i++) {
					sb.append("\t");
					position++;
				}
				sb.append(node.get(property));
			}
			
			int tabCount = colPositioner.getColumns().size() - position;
			for (int i = 0; i < tabCount; i++) {
				sb.append("\t");
			}
			sb.append("\n");
		}
		
		return sb.toString();
	}

	public void sortByGidAsc(List<NodeStructure> nodeStructures) {
		Collections.sort(nodeStructures, new Comparator<NodeStructure>() {
			@Override
			public int compare(NodeStructure o1, NodeStructure o2) {
				Integer gid1 = Integer.parseInt(o1.getMap().get("gid"));
				Integer gid2 = Integer.parseInt(o2.getMap().get("gid"));
				
				return gid1.compareTo(gid2);
			}
		});
	}

}
