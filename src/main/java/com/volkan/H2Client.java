package com.volkan;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.volkan.db.H2Helper;
import com.volkan.db.VJobEntity;

public class H2Client {

	private static final Logger logger = LoggerFactory.getLogger(H2Client.class);
	
	public void readClob(long jobID){
		H2Helper h2Helper;
		try {
			h2Helper = new H2Helper();
			VJobEntity vJobEntity = h2Helper.fetchJob(jobID);
			logger.info(vJobEntity.getVresult());
			h2Helper.closeConnection();
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void updateJobWithCypherResult(long jobID) {
		String cypherResult = "";
		try {
			cypherResult = readEntireFile("./src/main/resources/log/logFile.2012-12-24_19-35.log");
			H2Helper h2Helper = new H2Helper();
			h2Helper.updateJobWithResults(jobID, cypherResult);
			h2Helper.closeConnection();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		logger.info("updateJob finished");
	}
	
    private String readEntireFile(String filename) throws IOException {
        FileReader in = new FileReader(filename);
        StringBuilder contents = new StringBuilder();
        char[] buffer = new char[4096];
        int read = 0;
        do {
            contents.append(buffer, 0, read);
            read = in.read(buffer);
        } while (read >= 0);
        return contents.toString();
    }
	
	public long generateJob(Map<String, Object> jsonMap) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(jsonMap);
		H2Helper h2Helper = new H2Helper();
		long jobIDWithoutParent = 0;
		long newJobID = h2Helper.generateJob(jobIDWithoutParent, json);
		// This job does not have a parent which means its ID must be set as
		// PARENT_ID
		h2Helper.updateParentOfJob(newJobID);
		logger.info("newJobID = " + newJobID);
		h2Helper.closeConnection();
		return newJobID;
	}
}
