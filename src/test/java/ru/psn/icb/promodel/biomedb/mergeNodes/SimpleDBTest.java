package ru.psn.icb.promodel.biomedb.mergeNodes;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SimpleDBTest  {
	protected GraphDatabaseService graphDb;
	@Before
	public void prepareTestDatabase()
	{
	    graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
	}
	
    @After
    public void destroyTestDatabase()
    {
        graphDb.shutdown();
    }
    @Test
    public void startWithConfiguration()
    {
        // START SNIPPET: startDbWithConfig
        GraphDatabaseService db = new TestGraphDatabaseFactory()
            .newImpermanentDatabaseBuilder()
            .setConfig( GraphDatabaseSettings.pagecache_memory, "512M" )
            .setConfig( GraphDatabaseSettings.string_block_size, "60" )
            .setConfig( GraphDatabaseSettings.array_block_size, "300" )
            .newGraphDatabase();
        // END SNIPPET: startDbWithConfig
        db.shutdown();
    }

    @Test
    public void shouldCreateNodeTest()
    {
        // START SNIPPET: unitTest
        Node n = null;
        try ( Transaction tx = graphDb.beginTx() ){
            n = graphDb.createNode();
            n.setProperty( "name", "Nancy" );
            tx.success();
        }

        // The node should have a valid id
        assertThat( n.getId(), is( greaterThan( -1L ) ) );

        // Retrieve a node by using the id of the created node. The id's and
        // property should match.
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node foundNode = graphDb.getNodeById( n.getId() );
            assertThat( foundNode.getId(), is( n.getId() ) );
            assertThat( (String) foundNode.getProperty( "name" ), is( "Nancy" ) );
        }
        // END SNIPPET: unitTest
    }

}
