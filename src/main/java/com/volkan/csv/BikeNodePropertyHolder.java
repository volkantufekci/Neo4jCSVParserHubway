package com.volkan.csv;

public class BikeNodePropertyHolder implements NodePropertyHolder {

//	private String[] nodePropertyNames = {"__type__", "bikeId", "gid"};
	private String[] nodePropertyNames = {"__type__", "gid", "name"}; //should be given ordered
	
	public String[] getNodePropertyNames() {
		return nodePropertyNames;
	}
}
