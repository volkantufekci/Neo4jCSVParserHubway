package com.volkan.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class CSVColumnPositionerTest {

	private CSVColumnPositioner csvColumnPositioner;
	
	@Before
	public void setUp() throws Exception {
		csvColumnPositioner = new CSVColumnPositioner();
		csvColumnPositioner.addColumns(new StationNodePropertyHolder().getNodePropertyNames());
		System.out.println(csvColumnPositioner);
		csvColumnPositioner.addColumns(new TripNodePropertyHolder().getNodePropertyNames());
		csvColumnPositioner.addColumns(new BikeNodePropertyHolder().getNodePropertyNames());
		csvColumnPositioner.addColumns(new UserNodePropertyHolder().getNodePropertyNames());
		System.out.println(csvColumnPositioner);
	}

	@Test
	public final void testColumnOrdersForNodePropertyHolder() {
		NodePropertyHolder nodePropertyHolder = new StationNodePropertyHolder();
		String[] properties = nodePropertyHolder.getNodePropertyNames();
		
		Map<String, Integer> hmActual = 
						csvColumnPositioner.columnOrdersFor(nodePropertyHolder);
		
		assertNotNull(hmActual);
		
		assertEquals(properties.length, hmActual.size());
		
		for (String property : properties) {
			assertNotNull(hmActual.get(property));
		}
		
	}

}
