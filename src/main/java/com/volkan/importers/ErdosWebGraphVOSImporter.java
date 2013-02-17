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

public class ErdosWebGraphVOSImporter {

	public static void main(String[] args) {
		try {
			ErdosWebGraphVOSImporter.buildNETFileForPajek();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Converts the Erdos Web Graph TSV file into rels.txt file that VOSViewer understands. 
	 * @author volkan
	 * @throws IOException
	 */
	public static void readBigFile() throws IOException {
		Charset charset = Charset.forName("US-ASCII");

		BufferedWriter relFile = 
				new BufferedWriter(new FileWriter(Configuration.RELS_TXT_VOSVIEWER));
//		relFile.write("Start\tEnde\tType\n");

		BufferedReader reader = Files.newBufferedReader(Configuration.ERDOS_TSV, charset);
		Map<String, Integer> map = new HashMap<>();
		Integer index = 1;
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] splitted = line.split("\t");
			String first = splitted[0];
			String second = splitted[1];

			if (!map.containsKey(first)) {
				map.put(first, index++);
			}

			if (!map.containsKey(second)) {
				map.put(second, index++);
			}
			
			relFile.write(map.get(first) + "," + map.get(second) + ",1\n");

		}
		System.out.println(map.size());
		if (reader != null)
			reader.close();
		if (relFile != null)
			relFile.close();
	}
	
	public static void buildNETFileForPajek() throws IOException {
		Charset charset = Charset.forName("US-ASCII");

		BufferedWriter relFile = 
				new BufferedWriter(new FileWriter(Configuration.NET_FILE_PAJEK));
		relFile.write("*Vertices 1850065\n");
		relFile.write("*Arcslist\n");

		BufferedReader reader = Files.newBufferedReader(Configuration.ERDOS_TSV, charset);
		Map<String, Integer> map = new HashMap<>();
		Integer index = 1;
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] splitted = line.split("\t");
			String first = splitted[1];
			String second = splitted[0];

			if (!map.containsKey(first)) {
				map.put(first, index++);
			}

			if (!map.containsKey(second)) {
				map.put(second, index++);
			}
			
			relFile.write(map.get(first) + " " + map.get(second) + " 1\n");

		}
		System.out.println(map.size());
		if (reader != null)
			reader.close();
		if (relFile != null)
			relFile.close();
	}
}
