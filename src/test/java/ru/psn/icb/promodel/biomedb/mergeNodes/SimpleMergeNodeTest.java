package ru.psn.icb.promodel.biomedb.mergeNodes;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SimpleMergeNodeTest {
	protected GraphDatabaseService graphDb;
	private static String cypher = "CREATE (one {name:1}),"
			+ "(two {name: 2}),"
			+ "(another {name:1}),"
			+ "(tree {name:3}),"
			+ "(four {name:4}),"
			+ "one-[:T]->two,"
			+ "another-[:T]->three,"
			+ "one-[:T2]->three,"
			+ "one-[:T2]->four";

	private static GraphDatabaseService db;
	private Node toKeep, toRemove;

	@Before
	public void prepareTestDatabase() {
		graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

		graphDb.execute(cypher);
	}

	@After
	public void destroyTestDatabase() {
		graphDb.shutdown();
	}

	@Test
	public void shouldMergeNodes() {
		prepareNodes();

		NodeMerger nm=new NodeMerger(graphDb);
		nm.merge(toKeep,toRemove);
		try (Transaction tx = graphDb.beginTx()) {
			Node foundNode = graphDb.getNodeById(toKeep.getId());
			assertThat(foundNode.getId(), is(toKeep.getId()));
			assertThat((Long) foundNode.getProperty("name"), is(1L));
			assertThat(foundNode.getDegree(),is(4));
		}
	

	}

	void prepareNodes() {
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb
						.execute("match (n {name: 1}) return n, n.name")) {
			assertThat(result.hasNext(), is(true));
			Iterator<Node> n_column = result.columnAs("n");
			int i = 0;
			for (Node node : IteratorUtil.asIterable(n_column)) {
				if (i == 0)
					toKeep = node;
				else if (i == 1)
					toRemove = node;
				else {
					tx.failure();
					fail();
				}
				i += 1;
			}
		}
	}

	@Test
	public void shouldFindAllEdges(){
		prepareNodes();
		NodeMerger nm=new NodeMerger(graphDb);
		Map<Node, List<Relationship>> res=nm.getEdges(toRemove);
		assertEquals(1, res.size());
		assertThat(toKeep, not(isIn(res.keySet())));
		res=nm.getEdges(toKeep);
		assertEquals(3, res.size());
		assertThat(toRemove, not(isIn(res.keySet())));
//		fail();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void shouldThrowExceptionIfNodeNotFromDB(){
		NodeMerger nm=new NodeMerger(graphDb);
		Node n=null;
		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
		try (Transaction tx = db.beginTx()) {
		n=db.createNode();
		nm.checkNodeInDb(n);
		}
		fail("Should throw IllegalArgumentException");
	}
	

	@Test
	public void shouldCreateNodeTest() {
		// START SNIPPET: unitTest
		Node n = null;
		try (Transaction tx = graphDb.beginTx()) {
			n = graphDb.createNode();
			n.setProperty("name", "Nancy");
			tx.success();
		}

		// The node should have a valid id
		assertThat(n.getId(), is(greaterThan(-1L)));

		// Retrieve a node by using the id of the created node. The id's and
		// property should match.
		try (Transaction tx = graphDb.beginTx()) {
			Node foundNode = graphDb.getNodeById(n.getId());
			assertThat(foundNode.getId(), is(n.getId()));
			assertThat((String) foundNode.getProperty("name"), is("Nancy"));
		}
		// END SNIPPET: unitTest
	}

}
