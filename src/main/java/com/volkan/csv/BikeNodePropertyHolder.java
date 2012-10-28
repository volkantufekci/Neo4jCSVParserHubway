package com.volkan.csv;

public class BikeNodePropertyHolder implements NodePropertyHolder {

	private String[] nodePropertyNames = {"__type__", "bikeId", "gid"};
	
	public String[] getNodePropertyNames() {
		return nodePropertyNames;
	}
}
