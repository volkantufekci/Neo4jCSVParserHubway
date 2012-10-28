package com.volkan.csv;

public class TripNodePropertyHolder implements NodePropertyHolder {

	private String[] nodePropertyNames = {"__type__", "gid"};;
	
	public String[] getNodePropertyNames() {
		return nodePropertyNames;
	}

}
