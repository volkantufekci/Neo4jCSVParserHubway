package com.volkan.csv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Hede {

	public static void main(String[] args) {
		NodePropertyHolder[] nodeHolders = {
				new StationNodePropertyHolder(),
				new BikeNodePropertyHolder(),
				new TripNodePropertyHolder()
		};
		CSVColumnPositioner csvColumnPositioner = new CSVColumnPositioner();
		csvColumnPositioner.addNodeHolders(nodeHolders);
		
		HubwayCSVParser hubwayCSVParser = new HubwayCSVParser();		
		Map<String, Map<String, String>> stations = hubwayCSVParser.readStationsCSV();
		List<NodeStructure> stationNodeStructures = new ArrayList<NodeStructure>();
		for (Map<String, String> station : stations.values()) {
			stationNodeStructures.add(new NodeStructure(
											station, new StationNodePropertyHolder()));
		}
		
		//Write Headers to nodes.csv and rels.csv
		CSVWriterAsNeo4jStyle csvWriter = new CSVWriterAsNeo4jStyle();
		csvWriter.writeHeadersOfNodesCSV(csvColumnPositioner.toString());
		csvWriter.writeHeadersOfRelsCSV();
		
		
		String s = csvWriter.tabSeparate(stationNodeStructures, csvColumnPositioner);
		csvWriter.appendToNodesCSV(s);
		
		List<NodeStructure> tripsAndBikes = hubwayCSVParser.readTripsCSV(stations);
		s = csvWriter.tabSeparate(tripsAndBikes, csvColumnPositioner);
		csvWriter.appendToNodesCSV(s);
	}
}
