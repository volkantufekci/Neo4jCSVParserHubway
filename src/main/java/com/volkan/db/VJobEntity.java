package com.volkan.db;

public class VJobEntity {

	long id;
	long parent_id;
	String vquery;
	String vresult;
	boolean is_deleted;
	public long getId() {
		return id;
	}
	public long getParent_id() {
		return parent_id;
	}
	public String getVquery() {
		return vquery;
	}
	public String getVresult() {
		return vresult;
	}
	public boolean isIs_deleted() {
		return is_deleted;
	}
}
