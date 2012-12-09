package com.volkan.csv;

public class StationNodePropertyHolder implements NodePropertyHolder {

//	private String[] nodePropertyNames = {"__type__", "gid", "name", "stationId", "terminalName"};
	private String[] nodePropertyNames = {"__type__", "gid", "name"};
	
	public String[] getNodePropertyNames() {
		return nodePropertyNames;
	}
}
