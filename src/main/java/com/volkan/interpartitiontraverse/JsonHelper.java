package com.volkan.interpartitiontraverse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
}
