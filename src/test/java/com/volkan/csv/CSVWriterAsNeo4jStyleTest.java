package com.volkan.csv;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CSVWriterAsNeo4jStyleTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public final void testSortByGidAsc() {
		List<NodeStructure> nodeStructures = new ArrayList<NodeStructure>();
		
		Map<String, String> map1 = new HashMap<String, String>();
		map1.put("gid", "2");
		NodeStructure nodeStructure1 = new NodeStructure(map1, null);
		nodeStructures.add(nodeStructure1);
		
		Map<String, String> map2 = new HashMap<String, String>();
		map2.put("gid", "3");
		NodeStructure nodeStructure2 = new NodeStructure(map2, null);
		nodeStructures.add(nodeStructure2);
		
		Map<String, String> map3 = new HashMap<String, String>();
		map3.put("gid", "1");
		NodeStructure nodeStructure3 = new NodeStructure(map3, null);
		nodeStructures.add(nodeStructure3);
		
		CSVWriterAsNeo4jStyle testee = new CSVWriterAsNeo4jStyle();
		testee.sortByGidAsc(nodeStructures);
		
		String[] expected = {"1", "2", "3"};
		String[] actual = {nodeStructures.get(0).getMap().get("gid"),
				nodeStructures.get(1).getMap().get("gid"),
				nodeStructures.get(2).getMap().get("gid")
		};
		
		assertTrue(Arrays.equals(expected, actual));
		
	}

}
