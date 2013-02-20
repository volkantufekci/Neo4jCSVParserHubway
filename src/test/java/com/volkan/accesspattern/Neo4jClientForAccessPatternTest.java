package com.volkan.accesspattern;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ImpermanentGraphDatabase;

public class Neo4jClientForAccessPatternTest {

	private ImpermanentGraphDatabase db;
	private static final RelationshipType follows = DynamicRelationshipType.withName("follows");
	private Neo4jClientForAccessPattern neo4jClient;
	
	@Before
	public void setUp() throws Exception {
		db = new ImpermanentGraphDatabase();
        populateDb(db);
        neo4jClient = new Neo4jClientForAccessPattern();
	}

	@Test
	public final void testCollectNodeIDNeiIDsMap() throws IOException {
		Map<Long, List<Long>> actual = neo4jClient.collectNodeIDNeiIDsMap(db, 3);
		
		Map<Long, List<Long>> expected = new HashMap<Long, List<Long>>();
		expected.put(1l, Arrays.asList(2l,3l));
		expected.put(2l, Arrays.asList(1l));
		expected.put(3l, Arrays.asList(1l));
		
		assertEquals("Not matching maps", expected, actual);
	}
	
	@Test
	public final void testPutToNodeIDGidMap() throws IOException {
		Map<Long, Long> expected = new HashMap<>();
		expected.put(2l, 11l);
		expected.put(3l, 33l);
		
		neo4jClient.collectNodeIDNeiIDsMap(db, 3);
		Map<Long, Long> actual = neo4jClient.getNodeIDGidMap();
		
		assertEquals(expected, actual);
	}
	
    private void populateDb(GraphDatabaseService db) {
        Transaction tx = db.beginTx();
        try
        {
            Node refNode	= createRefNode(db, 555, 555);
            Node node1 		= createNormalNode(db, 11);
            Node node2 		= createNormalNode(db, 33);
            refNode.createRelationshipTo(node1, follows);
            refNode.createRelationshipTo(node2, follows);

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private Node createRefNode(GraphDatabaseService db, int randomID, int hashCode) {
//        Index<Node> people = db.index().forNodes("people");
        Node node = db.createNode();
        node.setProperty("randomID", randomID);
        node.setProperty("hashCode", hashCode);
//        people.add(node, "name", name);
        return node;
    }
    
    private Node createNormalNode(GraphDatabaseService db, int gid) {
//      Index<Node> people = db.index().forNodes("people");
      Node node = db.createNode();
      node.setProperty("gid", gid);
//      people.add(node, "name", name);
      return node;
  }

    @After
    public void tearDown() throws Exception {
        db.shutdown();

    }

}
