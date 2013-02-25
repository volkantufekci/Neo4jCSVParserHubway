package com.volkan.helpers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileHelper {

	private static final Logger logger = LoggerFactory.getLogger(FileHelper.class);
	
	public static void writeToFile(String content, String fileFullPath) throws IOException {

		BufferedWriter writer = new BufferedWriter(new FileWriter(fileFullPath));
		
		writer.write( content );
		
		if (writer != null)
			writer.close();
	}
	
	public static void deleteFilesUnderFile(File f) {
		logger.info("Deleting files under "+f.getName());
		for (File jsonFile : f.listFiles()) {
			jsonFile.delete();
		}
	}
	
	public static void main(String[] args) {
		String jsonsOutputDir = "src/main/resources/jsons/erdos/2depth/out_out";
		File f = new File(jsonsOutputDir);
		boolean b = f.mkdirs();
		System.out.println(b+ "hede");
	}
}
