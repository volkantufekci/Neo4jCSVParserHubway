package com.volkan.accesspattern;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class GPartPartitionerTest {

	private Map<Long, List<Long>> nodeIDNeiIDArrayMap;
	
	@Before
	public void setUp() throws Exception {
		nodeIDNeiIDArrayMap = new HashMap<Long, List<Long>>();
		nodeIDNeiIDArrayMap.put(1l, Arrays.asList(6l));
		nodeIDNeiIDArrayMap.put(2l, Arrays.asList(6l,7l));
		nodeIDNeiIDArrayMap.put(3l, Arrays.asList(6l,7l,8l));
		nodeIDNeiIDArrayMap.put(4l, Arrays.asList(6l,7l,8l));
		nodeIDNeiIDArrayMap.put(5l, Arrays.asList(8l));
		nodeIDNeiIDArrayMap.put(6l, Arrays.asList(1l,2l,3l,4l));
		nodeIDNeiIDArrayMap.put(7l, Arrays.asList(2l,3l,4l,4l));
		nodeIDNeiIDArrayMap.put(8l, Arrays.asList(4l,5l,3l));
		
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
