package com.volkan.db;

public class VJobEntity {

	long id;
	long parent_id;
	String vquery;
	String vresult;
	boolean is_deleted;
	java.sql.Timestamp created_at;
	java.sql.Timestamp finished_at;
	
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
	public java.sql.Timestamp getCreated_at() {
		return created_at;
	}
	public java.sql.Timestamp getFinished_at() {
		return finished_at;
	}
	public void setCreated_at(java.sql.Timestamp created_at) {
		this.created_at = created_at;
	}
	public void setFinished_at(java.sql.Timestamp finished_at) {
		this.finished_at = finished_at;
	}
}
