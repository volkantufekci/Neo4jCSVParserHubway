package com.volkan.interpartitiontraverse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.util.StringLogger;

import com.volkan.Utility;

public class TraverseHelper extends AbstractTraverseHelper{
	
//	private final StringLogger logger = StringLogger.logger(Utility.buildLogFileName());
	
	public List<String> traverse(GraphDatabaseService db, Map<String, Object> jsonMap) {
		List<String> realResults = new ArrayList<String>();
		TraversalDescription traversalDes = TraversalDescriptionBuilder.buildFromJsonMap(jsonMap);
		
//		8474'e(unpartitioned) ozgu olarak index yerine dogrudan id'den alinmali start_node
//		cunku 8474'te index yok!
//		Node startNode = db.getNodeById((int) jsonMap.get("start_node"));
		Node startNode = fetchStartNodeFromIndex(db, jsonMap);

		int toDepth = (Integer) jsonMap.get(JsonKeyConstants.DEPTH);
		for (Path path : traversalDes.traverse(startNode)) {
//			logger.logMessage(path.toString() + " # " + path.length(), true);
			Node endNode = path.endNode();
			if (didShadowComeInUnfinishedPath(toDepth, path, endNode)) {
				List<String> delegatedResults = delegateQueryToAnotherNeo4j(path, jsonMap);
				realResults.addAll( appendDelegatedResultsToPath( path, delegatedResults ));
			} else {
				if (path.length() >= toDepth) { //if it is a finished path
					realResults.add( appendEndingToFinishedPath(jsonMap, path, endNode) );
				} //else, a real node but unfinished path. No need to care
			}
		}

		return realResults;
	}

	private List<String> appendDelegatedResultsToPath(Path path, List<String> delegatedResults) {
		String port = getPortFromEndNode(path);
		List<String> results = new ArrayList<>();
		for (String delegatedResult : delegatedResults) {
			results.add( path + "~{" + port + "}" + delegatedResult );
		}
		
		return results;
	}

	private List<String> delegateQueryToAnotherNeo4j(Path path, Map<String, Object> jsonMap) {
		Map<String, Object> jsonMapClone = new HashMap<String, Object>();
		
		updateRelationships(path, jsonMap, jsonMapClone); 
		
		increaseHops(jsonMap, jsonMapClone);
		
		updateDepth(path, jsonMap, jsonMapClone);
		
		updateStartNode(path, jsonMapClone);
		
		List<String> resultList = delegateQueryOverRest(path, jsonMapClone);
		return resultList; 
	}
	

	private List<String> delegateQueryOverRest(Path path, Map<String, Object> jsonMapClone) {
		String port = getPortFromEndNode(path);
		RestConnector restConnector = new RestConnector(port);
		String jsonString = restConnector.delegateQuery(jsonMapClone);
		List<String> resultList = JsonHelper.convertJsonStringToList(jsonString);
		return resultList;
	}

}
