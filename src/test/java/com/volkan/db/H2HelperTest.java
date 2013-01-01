package com.volkan.db;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class H2HelperTest {
	
	private H2Helper h2Helper;
	
	@Before
	public void setUp() throws Exception {
		h2Helper = new H2Helper();
	}

	@After
	public void tearDown() throws Exception {
		h2Helper.closeConnection();
	}

	@Test
	public final void testUpdateJobWithResults() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testGenerateJob() {
		fail("Not yet implemented"); // TODO
	}
	
	@Test
	public final void testUpdateJobMarkAsDeleted() throws SQLException {
//		yeni bir job yarat, id'sini al
		long jobID = h2Helper.generateJob(0, "");
//		markAsDeleted()
		h2Helper.updateJobMarkAsDeleted(jobID);
//		fetch job
		VJobEntity vJobEntity = h2Helper.getJob(jobID);
//		assert is_deleted = true
		assertTrue("job with ID=" + jobID + " is not marked as deleted", vJobEntity.is_deleted);
	}

}
