package com.volkan.csv;

public class StationNodePropertyHolder implements NodePropertyHolder {

	private String[] nodePropertyNames = {"__type__", "gid", "name", "stationId", "terminalName"};
	
	public String[] getNodePropertyNames() {
		return nodePropertyNames;
	}
}
