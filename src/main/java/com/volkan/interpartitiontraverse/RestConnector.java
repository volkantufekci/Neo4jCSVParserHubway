package com.volkan.interpartitiontraverse;

import java.io.IOException;
import java.util.Map;

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
		//TODO BASE_URL, Properties'ten okunacak bir yapiya donusturulmeli
		urlHostAndPort = Configuration.BASE_URL_OF_NEO4J_INSTANCES + ":" + port + "/";
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
		}
		
		return result;
	}
	
}
