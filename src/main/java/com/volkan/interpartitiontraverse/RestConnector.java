package com.volkan.interpartitiontraverse;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.volkan.Configuration;

public class RestConnector {
	private String urlHostAndPort;
	
	public RestConnector(String url, String port) {
		urlHostAndPort = url + ":" + port + "/";
	}
	
	public RestConnector(String port) {
		urlHostAndPort = getNeo4jURLFromPropertiesForPort(port);
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
	private String getNeo4jURLFromPropertiesForPort(String port){
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
	
	public String delegateQuery(Map<String,Object> jsonMap){
		String result = "";
		Client client = Client.create();
		WebResource webResource = client.resource(urlHostAndPort + "example/service/volkan");
		ObjectMapper mapper = new ObjectMapper();
		
		try {
			ClientResponse clientResponse = 
					webResource.type("application/json")
							   .post(ClientResponse.class, mapper.writeValueAsString(jsonMap));
			
			result = clientResponse.getEntity(String.class);
		} catch (UniformInterfaceException | ClientHandlerException | IOException e) {
			e.printStackTrace();
		} 
		
		return result;
	}
	
	public String delegateQueryWithoutResult(Map<String,Object> jsonMap){
		String result = "";
		Client client = Client.create();
		WebResource webResource = client.resource(urlHostAndPort
				+ "example/service/volkan_async");
		ObjectMapper mapper = new ObjectMapper();

		try {
			ClientResponse clientResponse = 
					webResource.type("application/json")
					   .post(ClientResponse.class, mapper.writeValueAsString(jsonMap));
			result = clientResponse.getEntity(String.class);
		} catch (UniformInterfaceException | ClientHandlerException | IOException e) {
			e.printStackTrace();
			result = e.toString();
		}
		
		return result;
	}
	
}
