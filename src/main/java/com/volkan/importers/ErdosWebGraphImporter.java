package com.volkan.importers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import com.volkan.Configuration;

public class ErdosWebGraphImporter {

	public static void main(String[] args) throws IOException {
		readBigFile();
	}
	/**
	 * Converts the Erdos Web Graph TSV file into nodes.csv and rels.csv file that Neo4j 
	 * batch importer understands.
	 * @author volkan
	 * @throws IOException
	 */
	public static void readBigFile() throws IOException {
		Charset charset = Charset.forName("US-ASCII");

		BufferedWriter nodeFile = new BufferedWriter(new FileWriter(Configuration.NODES_CSV));
		nodeFile.write("Node\tCounter:int\n");
		BufferedWriter relFile = new BufferedWriter(new FileWriter(Configuration.RELS_CSV));
		relFile.write("Start\tEnde\tType\n");

		BufferedReader reader = Files.newBufferedReader(Configuration.ERDOS_TSV, charset);
		Map<String, Integer> map = new HashMap<>();
		Integer index = 1, selfRefCount = 0;
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] splitted = line.split("\t");
			String first = splitted[0];
			String second = splitted[1];

			if (first.equalsIgnoreCase(second)) {
				selfRefCount++;
				continue;
			}
			
			if (!map.containsKey(first)) {
				nodeFile.write(first + "\t" + index + "\n");
				map.put(first, index++);
			}

			if (!map.containsKey(second)) {
				nodeFile.write(second + "\t" + index + "\n");
				map.put(second, index++);
			}
			
			relFile.write(map.get(second) + "\t" + map.get(first) + "\tfollows\n");

		}
		
		System.out.println("SelfRefCount: "+selfRefCount);
		System.out.println("Total node size= "+map.size());
		
		if (reader != null)
			reader.close();
		if (nodeFile != null)
			nodeFile.close();
		if (relFile != null)
			relFile.close();
	}
}
