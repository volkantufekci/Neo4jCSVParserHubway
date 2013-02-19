package com.volkan.accesspattern;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class GPartPartitionerTest {

	private Map<Integer, List<Integer>> nodeIDNeiIDArrayMap;
	
	@Before
	public void setUp() throws Exception {
		nodeIDNeiIDArrayMap = new HashMap<Integer, List<Integer>>();
		nodeIDNeiIDArrayMap.put(1, Arrays.asList(6));
		nodeIDNeiIDArrayMap.put(2, Arrays.asList(6,7));
		nodeIDNeiIDArrayMap.put(3, Arrays.asList(6,7,8));
		nodeIDNeiIDArrayMap.put(4, Arrays.asList(6,7,8));
		nodeIDNeiIDArrayMap.put(5, Arrays.asList(8));
		nodeIDNeiIDArrayMap.put(6, Arrays.asList(1,2,3,4));
		nodeIDNeiIDArrayMap.put(7, Arrays.asList(2,3,4,4));
		nodeIDNeiIDArrayMap.put(8, Arrays.asList(4,5,3));
		
	}

	@Test
	public final void testGenerateGrfFileContent() {
		String expected = "0\n" +
						  "8\t20\n" +
						  "1\t000\n" +
						  "1\t6\n" +
						  "2\t6\t7\n" +
						  "3\t6\t7\t8\n" +
						  "3\t6\t7\t8\n" +
						  "1\t8\n" +
						  "4\t1\t2\t3\t4\n" +
						  "3\t2\t3\t4\n" +
						  "3\t3\t4\t5";
		String actual = GPartPartitioner.generateGrfFileContent(nodeIDNeiIDArrayMap);
		assertTrue("GrfFileContent is not as expected\nExpected:\n" 
					+ expected + "\nActual:\n" + actual, 
					expected.equalsIgnoreCase(actual));
	}

}
