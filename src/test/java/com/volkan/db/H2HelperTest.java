package com.volkan.db;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class H2HelperTest {
	
	private H2Helper h2Helper;
	
	@Before
	public void setUp() throws Exception {
		h2Helper = new H2Helper(" VTEST ");
		h2Helper.deleteAll();
	}

	@After
	public void tearDown() throws Exception {
		h2Helper.closeConnection();
	}

	@Test
	public final void testUpdateJobWithResults() throws SQLException {
		long parentID = -1;
		long jobID = h2Helper.generateJob(parentID, "");
		String expected = "cypherResults";
		h2Helper.updateJobWithResults(jobID, expected);
		VJobEntity vJobEntity = h2Helper.fetchJob(jobID);
		assertTrue(expected.equalsIgnoreCase(vJobEntity.getVresult()));
	}

	@Test
	public final void testGenerateJob() throws SQLException {
		long parentID = -1;
		long jobID = h2Helper.generateJob(parentID, "");
		VJobEntity vJobEntity = h2Helper.fetchJob(jobID);
		assertNotNull(vJobEntity);
	}
	
	@Test
	public final void testUpdateJobMarkAsDeleted() throws SQLException {
//		yeni bir job yarat, id'sini al
		long jobID = h2Helper.generateJob(0, "");
//		markAsDeleted()
		h2Helper.updateJobMarkAsDeleted(jobID);
//		fetch job
		VJobEntity vJobEntity = h2Helper.fetchJob(jobID);
//		assert is_deleted = true
		assertTrue("job with ID=" + jobID + " is not marked as deleted", vJobEntity.is_deleted);
	}

	@Test
	public final void testFetchJobNotDeletedWithParentID() throws SQLException {
		long parentID = -1;
		long jobID = h2Helper.generateJob(parentID, "");
		h2Helper.updateJobWithResults(jobID, "cypherResults");
		List<VJobEntity> results = h2Helper.fetchJobNotDeletedWithParentID(parentID);
		assertEquals(jobID, results.get(0).id);
		h2Helper.closeConnection();
	}
	
	@Test
	public final void testUpdateJobsMarkAsDeletedFor1Job() throws SQLException {
//		yeni bir job yarat, id'sini al
		long jobID = h2Helper.generateJob(0, "");
//		markAsDeleted()
		h2Helper.updateJobsMarkAsDeleted(Arrays.asList(jobID));
//		fetch job
		VJobEntity vJobEntity = h2Helper.fetchJob(jobID);
//		assert is_deleted = true
		assertTrue("job with ID=" + jobID + " is not marked as deleted", vJobEntity.is_deleted);
	}

	@Test
	public final void testUpdateJobsMarkAsDeletedForMultipleJobs() throws SQLException {
//		yeni joblar yarat, id'sini al
		long jobID1 = h2Helper.generateJob(0, "");
		long jobID2 = h2Helper.generateJob(0, "");
//		markAsDeleted()
		h2Helper.updateJobsMarkAsDeleted(Arrays.asList(jobID1, jobID2));
//		fetch jobs
		VJobEntity vJobEntity1 = h2Helper.fetchJob(jobID1);
		VJobEntity vJobEntity2 = h2Helper.fetchJob(jobID2);
//		assert is_deleted = true
		assertTrue("job with ID=" + jobID1 + " is not marked as deleted", vJobEntity1.is_deleted);
		assertTrue("job with ID=" + jobID2 + " is not marked as deleted", vJobEntity2.is_deleted);
	}
	
	@Test
	public final void testBuildWhereClauseForIDList() throws SQLException {
		List<Long> jobIDs = Arrays.asList(1l, 2l, 3l);
		String expected = " (ID = ? OR ID = ? OR ID = ?) ";
		String actual = h2Helper.buildWhereClauseForIDList(jobIDs);
		assertTrue("Expected:"+expected+" is not equal to actual:"+actual, 
						expected.equalsIgnoreCase(actual));
		
		jobIDs = Arrays.asList(1l);
		expected = " (ID = ?) ";
		actual = h2Helper.buildWhereClauseForIDList(jobIDs);
		assertTrue("Expected:"+expected+" is not equal to actual:"+actual, 
						expected.equalsIgnoreCase(actual));
	}
	
}
