package com.volkan;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class Utility {

	public static String buildLogFileName () {
//		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddhhmm");
//		return "hede" + simpleDateFormat.format(new Date());
		return "vlog";
	}

	/**
	 * Reads the interpartitiontraverse.properties located under the Neo4j_Instance/conf directory
	 * in order to get the URL of the Neo4j instance distinguished by the @param port.<br> 
	 * Actually, in the distributed scenario port should be thought as the ID of Neo4j instance
	 * which runs on standart port(6474) instead of the given port parameter.<br> 
	 * But in local scenario "port" parameter is used both as ID of the Neo4j instance and the port
	 * it is running on.<br>
	 * If there occurrs an error default URL(which may be localhost) taken from Configuration.java 
	 * is returned.
	 * @author volkan
	 * @param port
	 * @return URL of the Neo4j instance distinguished by the param port
	 */
	public static String getNeo4jURLFromPropertiesForPort(String port){
		String defaultValue = Configuration.BASE_URL_OF_NEO4J_INSTANCES + ":" + port + "/";
		String propertyValue = defaultValue;
		
		FileInputStream in = null;
		try {
			in = new FileInputStream("conf/interpartitiontraverse.properties");
			Properties properties = new Properties();
			properties.load(in);
			propertyValue = properties.getProperty(port, defaultValue);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return propertyValue;
	}

}
