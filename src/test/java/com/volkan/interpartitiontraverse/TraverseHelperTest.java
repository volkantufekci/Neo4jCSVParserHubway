package com.volkan.interpartitiontraverse;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.volkan.interpartitiontraverse.TraverseHelper;

public class TraverseHelperTest {

	TraverseHelper traverseHelper;
	@Before
	public void setUp() throws Exception {
		traverseHelper = new TraverseHelper();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public final void testIncreaseHops() {
		Map<String, Object> jsonMap = buildJsonMap();
		Map<String, Object> jsonMapClone = new HashMap<>();
		traverseHelper.increaseHops(jsonMap, jsonMapClone);
		int expected = (int) jsonMap.get("hops") + 1;
		int actual   = (int) jsonMapClone.get("hops");
		assertEquals("Hop is not increased", expected, actual);
	}
	
	@Test
	public final void testPruneRelationships() {
		Map<String, Object> jsonMap = buildJsonMap();
		int fromDepth 	= 1;
		@SuppressWarnings("unchecked")
		List<Map<String, String>> actualRelationships = 
				traverseHelper.pruneRelationships(
						(List<Map<String, String>>) jsonMap.get("relationships"), fromDepth);
		
		Map<String,String> rel2 = new HashMap<String,String>();
		rel2.put("type", "BIKE");
		rel2.put("direction", "IN");

		Map<String,String> rel3 = new HashMap<String,String>();
		rel3.put("type", "END");
		rel3.put("direction", "OUT");
		
		List<Map<String, String>> expectedRelationships = new ArrayList<Map<String, String>>();
		expectedRelationships.add(rel2); 
		expectedRelationships.add(rel3);
		
		assertEquals("rels are not same", expectedRelationships, actualRelationships);
	}
	
	private Map<String, Object> buildJsonMap() {
		Map<String,Object> userData = new HashMap<String,Object>();
		
		Map<String,String> rel1 = new HashMap<String,String>();
		rel1.put("type", "BIKE");
		rel1.put("direction", "OUT");
		
		Map<String,String> rel2 = new HashMap<String,String>();
		rel2.put("type", "BIKE");
		rel2.put("direction", "IN");

		Map<String,String> rel3 = new HashMap<String,String>();
		rel3.put("type", "END");
		rel3.put("direction", "OUT");
		
		List<Map<String, String>> relationships = new ArrayList<Map<String, String>>();
		relationships.add(rel1); 
		relationships.add(rel2);
		relationships.add(rel3);
		
		userData.put("relationships", relationships);
		userData.put("client", "NEO4J");
		userData.put(JsonKeyConstants.DEPTH, 3);
		userData.put("start_node", 12);
		userData.put("hops", 0);
		return userData;
	}

}
