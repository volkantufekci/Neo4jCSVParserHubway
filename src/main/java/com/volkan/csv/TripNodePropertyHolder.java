package com.volkan.csv;

public class TripNodePropertyHolder implements NodePropertyHolder {

//	private String[] nodePropertyNames = {"__type__", "gid"};
	private String[] nodePropertyNames = {"__type__", "gid", "name"};
	
	public String[] getNodePropertyNames() {
		return nodePropertyNames;
	}

}
