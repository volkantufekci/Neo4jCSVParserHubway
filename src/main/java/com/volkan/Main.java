package com.volkan;

import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.volkan.csv.NodePropertyHolder;
import com.volkan.csv.StationNodePropertyHolder;

public class Main {

//	private static final String DB_PATH = "target/matrix-db";
	private static final String DB_PATH = 
//			"/home/volkan/Development/tez/Neo4jSurumleri/neo4j-community-1.8.M07_1/data/graph.db";
		"/home/volkan/Documents/workspace-sts-2.7.2.RELEASE/neowithmaven20121016/src/main/resources/graph.db";
	private static GraphDatabaseService db;
	
	public static void main(String[] args) {
		
		db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		registerShutdownHook();
		
		// add some data first
		Transaction tx = db.beginTx();
		try
		{
		    Node refNode = db.getReferenceNode();
		    refNode.setProperty( "name", "reference node" );
		    tx.success();
		}
		finally
		{
		    tx.finish();
		}
		 
		ExecutionEngine engine = new ExecutionEngine( db );
//		testMostUsedStations(engine);
//		testPrintingOrdersOfColumns(engine);
//		test10NodeFetch(engine);
		test10RelFetch(engine);
	}

	private static void testMostUsedStations(ExecutionEngine engine) {
		ExecutionResult result = engine.execute( "START stat=node:Station(\"stationId:*\") "
				+ " MATCH stat<-[:`START`]-()-[:END]->endstat "
				+ " RETURN stat.name,stat.stationId,endstat.name,endstat.stationId,count(*)"
				+ " ORDER BY count(*) DESC LIMIT 15;" );
		
		System.out.println( result );

		String rows = "";
		for ( Map<String, Object> row : result )
		{
		    for ( Entry<String, Object> column : row.entrySet() )
		    {
		        rows += column.getKey() + ": " + column.getValue() + " | ";
		    }
		    rows += "\n";
		}
		System.out.println( rows );
	}


	private static void test10RelFetch(ExecutionEngine engine) {
		StringBuilder sb = new StringBuilder();
		for (int i = 1000; i < 1010; i++) {
			sb.append(i + ",");
		}
		sb.deleteCharAt(sb.length() - 1); //remove last ","
		
		ExecutionResult result = engine.execute( 
				  "START stat=relationship(" + sb.toString() + ")"
				+ "RETURN stat;" );
		
		System.out.println( result );

		for ( Map<String, Object> row : result )
		{
			Relationship rel = (Relationship) row.values().iterator().next();

			Node startNode = rel.getStartNode();
			Node endNode   = rel.getEndNode();

			System.out.println(startNode.getId() + "-[:" + rel.getType() + "]->" + endNode.getId());
		}
	}	
	
	private static void test10NodeFetch(ExecutionEngine engine) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 100000; i++) {
			sb.append(i + ",");
		}
		sb.deleteCharAt(sb.length() - 1); //remove last ","
		
		ExecutionResult result = engine.execute( 
				  "START stat=node(" + sb.toString() + ")"
				+ "RETURN stat;" );
		
		System.out.println( result );

		for ( Map<String, Object> row : result )
		{
			Node node = (Node) row.values().iterator().next();
			
//			NodePropertyHolder stationNodePropertyHolder = new StationNodePropertyHolder();
//			for (String prop : stationNodePropertyHolder.getNodePropertyNames()) {
//				System.out.println(prop + " => " + node.getProperty(prop));
//			}
			
			for (String prop : node.getPropertyKeys()) {
			    System.out.println(prop + " => " + node.getProperty(prop));
			}
		}
	}
	
	private static void testPrintingOrdersOfColumns(ExecutionEngine engine) {
		ExecutionResult result = engine.execute( 
				  "START stat=node:Station(\"stationId:*\") "
				+ "RETURN stat LIMIT 15;" );
		
		System.out.println( result );

		for ( Map<String, Object> row : result )
		{
			Node node = (Node) row.values().iterator().next();
			
			NodePropertyHolder stationNodePropertyHolder = new StationNodePropertyHolder();
			for (String prop : stationNodePropertyHolder.getNodePropertyNames()) {
				System.out.println(prop + " => " + node.getProperty(prop));
			}
			
//			for (String prop : node.getPropertyKeys()) {
//			    System.out.println(prop + " => " + node.getProperty(prop));
//			}
		}
	}

//	List<String> columns = result.columns();
//	System.out.println( columns );
	
	
//	This outputs:
//
//	[n, n.name]
//	To fetch the result items in a single column, do like this:
//
//	Iterator<Node> n_column = result.columnAs( "n" );
//	for ( Node node : IteratorUtil.asIterable( n_column ) )
//	{
//	    // note: we're grabbing the name property from the node,
//	    // not from the n.name in this case.
//	    String nodeResult = node + ": " + node.getProperty( "name" );
//	    System.out.println( nodeResult );
//	}
//	In this case there’s only one node in the result:
	
    private static void registerShutdownHook()
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime()
        .addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                db.shutdown();
            }
        } );
    }
}
