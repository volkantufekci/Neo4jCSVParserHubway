package com.volkan;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.db.H2Helper;
import com.volkan.db.VJobEntity;
import com.volkan.interpartitiontraverse.RestConnector;

public class Neo4jClientAsync {

	private static final Logger logger = LoggerFactory.getLogger(Neo4jClientAsync.class);
	
	public void delegateQueryAsync(final String port, final Map<String, Object> jsonMap) {

//		Thread t = new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
		RestConnector restConnector = new RestConnector(port);
		logger.info("uzaktan:" + restConnector.delegateQueryWithoutResult(jsonMap));
//			}
//		});
//		t.start();
		
	}
	
    public void periodicFetcher(long parentID) throws ClassNotFoundException, SQLException, InterruptedException {
        long start = System.currentTimeMillis();

        H2Helper h2Helper = new H2Helper();
        boolean atLeast1ResultFetched = true;
		while (true) {
			Thread.sleep(500);

			List<VJobEntity> list = h2Helper.fetchJobNotDeletedWithParentID(parentID);
			if (list.isEmpty() && atLeast1ResultFetched) {
				break;
			} else {
				List<Long> jobIDs = new ArrayList<Long>();
				for (VJobEntity vJobEntity : list) {
					if (vJobEntity.getVresult() != null){
						logger.info(vJobEntity.getVresult());
						jobIDs.add(vJobEntity.getId());
					}	
				}
				if (!jobIDs.isEmpty())
					h2Helper.updateJobsMarkAsDeleted(jobIDs);
				// atLeast1ResultFetched = true;
			}
		}
        h2Helper.closeConnection();

        long end = System.currentTimeMillis();
        logger.info(end - start + " miliseconds passed in periodicFetcher" );
    }
}
