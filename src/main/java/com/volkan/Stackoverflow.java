package com.volkan;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Stackoverflow {

	 public static final String DBPATH="CQL";

	    public static void main(String args[])
	    {
	        GraphDatabaseService path=new EmbeddedGraphDatabase(DBPATH);
	        Transaction tx=path.beginTx();
	        try
	        {
	        Map<String, Object> props  = new HashMap<String, Object>();
	        props .put( "Firstnamename", "Sharon" );
	        props .put( "lastname", "Eunis" );

	        Map<String, Object> params = new HashMap<String, Object>();
	        params.put( "props", props  );

	        ExecutionEngine engine=new ExecutionEngine(path);
	        ExecutionResult result=engine.execute( "create ({props})", params );
	        System.out.println(result);
	        tx.success();
	        } 
	        finally
	        {
	             tx.finish();
	             path.shutdown();

	        }
	    }
}
