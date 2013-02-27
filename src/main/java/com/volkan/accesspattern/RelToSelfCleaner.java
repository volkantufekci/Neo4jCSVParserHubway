package com.volkan.accesspattern;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

public class RelToSelfCleaner {

	private static final String DB_PATH = System.getProperty("user.home") +  
			"/Development/tez/Neo4jSurumleri/neo4j-community-1.8.M07erdos/data/graph.db/";
	private static GraphDatabaseService db;
	private static final int MAX_NODE_COUNT 	 = 1850065;
	
	public static void main(String[] args) {
		db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook();
		
		TraversalDescription td = Traversal.description();
		td = td.relationships(DynamicRelationshipType.withName("follows"), Direction.BOTH);
		td = td.evaluator(Evaluators.toDepth(1));
		
		int deletedCount = 0;
		for (int i = 1; i <= MAX_NODE_COUNT; i++) {
			Node startNode = db.getNodeById(i);
			Transaction tx = db.beginTx();
			for (Relationship rel : startNode.getRelationships()) {
				if (rel.getOtherNode(startNode).getId() == startNode.getId()) {
					rel.delete();
					deletedCount++;
				}
			}
			tx.success();
			tx.finish();
		}
		System.out.println("Toplam kendine ref veren ve silinen:" + deletedCount);
	}
	
	private static void registerShutdownHook() {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running example before it's completed)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				db.shutdown();
			}
		});
	}
}
