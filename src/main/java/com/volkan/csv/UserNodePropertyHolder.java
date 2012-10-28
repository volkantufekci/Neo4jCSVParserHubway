package com.volkan.csv;

public class UserNodePropertyHolder implements NodePropertyHolder {

	private String[] nodePropertyNames = {"__type__", "name"};
	
	public String[] getNodePropertyNames() {
		return nodePropertyNames;
	}

}
