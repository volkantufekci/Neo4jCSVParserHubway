package com.volkan.csv;

public class UserNodePropertyHolder implements NodePropertyHolder {

//	private String[] nodePropertyNames = {"__type__", "name"};
	private String[] nodePropertyNames = {"__type__", "gid", "name"};
	
	public String[] getNodePropertyNames() {
		return nodePropertyNames;
	}

}
