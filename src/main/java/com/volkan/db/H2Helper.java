package com.volkan.db;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class H2Helper {

	private Connection con;

	public void updateJobWithResults(long jobID, String cypherResult) throws SQLException {
		String sql = "UPDATE VTEST SET VRESULT = ? WHERE ID = ?";
		PreparedStatement prepStatement = null;
		try {
			prepStatement = con.prepareStatement(sql);
			prepStatement.setClob(1, new StringReader(cypherResult));
			prepStatement.setLong(2, jobID);
			int affectedRows = prepStatement.executeUpdate();
			if (affectedRows == 0) {
				throw new SQLException("Creating job failed, no rows affected.");
			}
		} catch (Exception e) {
			throw new SQLException("Job could not be updated", e);
		}
		
		if(prepStatement != null)
			prepStatement.close();
	}

	public void updateParentOfJob(long jobID) throws SQLException {
		String sql = "UPDATE VTEST SET PARENT_ID = ? WHERE ID = ?";
		PreparedStatement prepStatement = con.prepareStatement(sql);
		prepStatement.setLong(1, jobID);
		prepStatement.setLong(2, jobID);
		int affectedRows = prepStatement.executeUpdate();
		if (affectedRows == 0) {
			throw new SQLException(
					"Updating parent of job failed, no rows affected.");
		}
		
		if(prepStatement != null)
			prepStatement.close();
	}
	
	public long generateJob(long parentJobID, String traversalQuery) {
		ResultSet resultSet = null;
		PreparedStatement prepStatement = null;
		long generatedJobID = 0;
		try {
			// prepared statement
			prepStatement = con.prepareStatement("INSERT INTO VTEST (PARENT_ID, VQUERY) " +
												 "VALUES (?,?)");
			prepStatement.setLong(1, parentJobID);
			prepStatement.setString(2, traversalQuery);
			int affectedRows = prepStatement.executeUpdate();
			if (affectedRows == 0) {
				throw new SQLException("Creating job failed, no rows affected.");
			}

			resultSet = prepStatement.getGeneratedKeys();
			if (resultSet.next()) {
				generatedJobID = resultSet.getLong(1);
			} else {
				throw new SQLException("Creating job failed, no generated key obtained.");
			}
			
			updateParentOfJob(generatedJobID);
			
			try {
				resultSet.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			try {
				prepStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return generatedJobID;
	}
	
	public H2Helper() throws ClassNotFoundException, SQLException {
		con = getConnection();
	}

	private Connection getConnection() throws ClassNotFoundException, SQLException {
		// driver for H2 db get from http://www.h2database.com
		Class.forName("org.h2.Driver");

		// Connection con = DriverManager.getConnection("jdbc:h2:mem:mytest", "sa", "");
		Connection con = DriverManager.getConnection("jdbc:h2:tcp://localhost/~/test", "sa","");
		return con;
	}
	
	public void closeConnection() throws SQLException {
		con.close();
	}
}

// insert 10 row data
//for (int i = 0; i < 10; i++) {
//	prep.setLong(1, i);
//	prep.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
//	prep.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
//	prep.setString(4, "Activity-" + i);
//
//	// batch insert
//	prep.addBatch();
//}
//con.setAutoCommit(false);
//prep.executeBatch();
//con.setAutoCommit(true);
//
//// query to database
//try {
//	ResultSet rs = stat
//			.executeQuery("Select STARTTIME, ENDTIME, ACTIVITY_NAME from ACTIVITY");
//	while (rs.next()) {
//
//		Date start = rs.getTimestamp(1);
//		Date end = rs.getTimestamp(2);
//		String activityName = rs.getString(3);
//
//		// print query result to console
//		System.out.println("activity: " + activityName);
//		System.out.println("start: " + start);
//		System.out.println("end: " + end);
//		System.out.println("--------------------------");
//	}
//	rs.close();
//} catch (SQLException e) {
//	e.printStackTrace();
//}