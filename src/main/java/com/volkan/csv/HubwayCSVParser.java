package com.volkan.csv;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.volkan.Configuration;

public class HubwayCSVParser {
	
	private int gid = 1;
	private NodePropertyHolder tripNodePropertyHolder = new TripNodePropertyHolder();
	private NodePropertyHolder bikeNodePropertyHolder = new BikeNodePropertyHolder();
	private CSVWriterAsNeo4jStyle csvWriter = new CSVWriterAsNeo4jStyle(); 
	
	public List<NodeStructure> readTripsCSV(Map<String, Map<String, String>> stations){
		InputStream fis; BufferedReader br; String line;

		List<NodeStructure> tripsAndBikes = new ArrayList<NodeStructure>();
		Map<String, Map<String, String>> bikes = new HashMap<String, Map<String, String>>();
		
		try {
			fis = new FileInputStream(Configuration.HUBWAY_TRIPS_CSV);
			br  = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
			while ((line = br.readLine()) != null) {
			    String[] splitted = line.split(",");
				String startStationId 	= splitted[4];
				String endStationId 	= splitted[6];
				Map<String, String> startStation = stations.get(startStationId);
				Map<String, String> endStation 	 = stations.get(endStationId);

				if (startStation == null || endStation == null) {
					continue;
				}
				
				Map<String, String> trip = buildTripMap();
				tripsAndBikes.add(tripAsNodeStructure(trip));
				
				csvWriter.appendToRelsCSV(trip.get("gid"), startStation.get("gid"), "START");
				csvWriter.appendToRelsCSV(trip.get("gid"), endStation.get("gid"), "END");				

				//bike
				String bikeId 	= splitted[7];
				Map<String, String> bike = bikes.get(bikeId);
				if (bike == null) {
					bike = buildBikeMap(bikeId);
					tripsAndBikes.add(bikeAsNodeStructure(bike));
					bikes.put(bikeId, bike);
				} 
				
				//trip  -[:BIKE]-> bike relation
				csvWriter.appendToRelsCSV(trip.get("gid"), bike.get("gid"), "BIKE");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return tripsAndBikes;
	}
	
	private NodeStructure bikeAsNodeStructure(Map<String, String> bike) {
		return new NodeStructure(bike, bikeNodePropertyHolder);
	}
	private NodeStructure tripAsNodeStructure(Map<String, String> trip) {
		return new NodeStructure(trip, tripNodePropertyHolder);
	}
	private Map<String, String> buildTripMap() {
		Map<String, String> trip = new HashMap<String, String>();
		trip.put("__type__", "Trip");
		trip.put("gid", gid++ + "");
		return trip;
	}
	private Map<String, String> buildBikeMap(String bikeId) {
		Map<String, String> bike = new HashMap<String, String>();
		bike.put("__type__", "Bike");
		bike.put("gid", gid++ + "");
		bike.put("bikeId", bikeId);
		return bike;
	}	
	public Map<String, Map<String, String>> readStationsCSV(){
		InputStream    fis;
		BufferedReader br;
		String         line;

		Map<String, Map<String, String>> stations = new HashMap<String, Map<String,String>>();
		
		try {
			fis = new FileInputStream(Configuration.HUBWAY_STATIONS_CSV);
			br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
			while ((line = br.readLine()) != null) {
			    String[] splitted = line.split(",");
				String stationId 	= splitted[0];
				String terminalName = splitted[1];
				String name			= splitted[2];
				
				Map<String, String> station = new HashMap<String, String>();
				station.put("__type__", "Station");
				station.put("stationId", stationId);
				station.put("name", name);
				station.put("terminalName", terminalName);
				station.put("gid", gid++ + "");
				
				stations.put(stationId, station);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return stations;
	}
	
}
