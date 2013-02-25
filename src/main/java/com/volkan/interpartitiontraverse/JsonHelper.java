package com.volkan.interpartitiontraverse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

public class JsonHelper {

	public static List<String> convertJsonStringToList(String jsonString) {
		ObjectMapper mapper = new ObjectMapper();
		List<String> resultList = new ArrayList<>();
		try {
			@SuppressWarnings("unchecked")
			List<String> jsonList = mapper.readValue(jsonString, List.class);
			resultList.addAll(jsonList);
		} catch (IOException e) {
			resultList.add("jsonString could not be read" + e + "\n" + jsonString);
		}
		return resultList;
	}
	

	@SuppressWarnings("unchecked")
	public static Map<String, Object> readJsonFileIntoMap(String fileName) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> jsonMap	= mapper.readValue(new File(fileName), Map.class);
		return jsonMap;
	}
	
	public static String writeMapIntoJsonString(Object jsonMap) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		String jsonString = mapper.writeValueAsString(jsonMap);
		return jsonString;
	}
	
	public static Map<String, Object> createJsonMapWithDirectionsAndRelTypes(
			List<String> directions, List<String> relTypes) 
	{
		Map<String, Object> jsonMap = new HashMap<String, Object>();
		
		List<Map<String, Object>> relList = new ArrayList<>();
		for (int i = 0; i < directions.size(); i++) {
			Map<String, Object> rel1Map = new HashMap<String, Object>();
			rel1Map.put("direction", directions.get(i));
			rel1Map.put("type", relTypes.get(i));
			relList.add(rel1Map);
		}
		
		jsonMap.put("relationships", relList);
		jsonMap.put("depth", relList.size());
		
		jsonMap.put(JsonKeyConstants.JOB_ID, 0);
		jsonMap.put(JsonKeyConstants.PARENT_JOB_ID, 0);
		jsonMap.put(JsonKeyConstants.PATH, "");
		
		return jsonMap;
	}
	
	public static void main(String[] args) throws Exception {
		Map<String, Object> jsonMap = new HashMap<String, Object>();
		
		jsonMap.put("start_node", 123);
		
		List<Map<String, Object>> relList = new ArrayList<>();
		Map<String, Object> rel1Map = new HashMap<String, Object>();
		Map<String, Object> rel2Map = new HashMap<String, Object>();
		rel1Map.put("direction", "OUT");
		rel1Map.put("type", "follows");
		rel2Map.put("direction", "OUT");
		rel2Map.put("type", "follows");
		relList.add(rel1Map); relList.add(rel2Map);
		jsonMap.put("relationships", relList);
		jsonMap.put("depth", relList.size());
		
		jsonMap.put(JsonKeyConstants.JOB_ID, 0);
		jsonMap.put(JsonKeyConstants.PARENT_JOB_ID, 0);
		jsonMap.put(JsonKeyConstants.PATH, "");
		
		System.out.println(writeMapIntoJsonString(jsonMap));
	}
}
