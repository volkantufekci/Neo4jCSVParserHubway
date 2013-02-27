package com.volkan.accesspattern;

public class EdgeWithWeight {

	int weight;
	long otherNodeID;
	
	public int getWeight() {
		return weight;
	}
	public long getOtherNodeID() {
		return otherNodeID;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	public void setOtherNodeID(long otherNodeID) {
		this.otherNodeID = otherNodeID;
	}
}
