package ru.psn.icb.promodel.biomedb.exe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;

import ru.psn.icb.promodel.biomedb.mergeNodes.CantMergeException;
import ru.psn.icb.promodel.biomedb.mergeNodes.NodeMerger;

public class MergeTerms {
	public static String DB_PATH = "/Users/lptolik/Documents/Neo4j/data/graph.db";
	public static String GET_TRIP_TEXT = "match (t:Term) "
			+ "with t.text as text,count(t) as cnt "
			+ "where cnt >1 return text,cnt " + "order by cnt desc";
	public static String GET_TERMS = "match (t:Term) where t.text={txt} return t order by id(t)";

	private GraphDatabaseService graphDb;

	private NodeMerger nMerger;

	public static void main(String[] args) throws IOException {
		MergeTerms mt = new MergeTerms();
		mt.createDb();
		mt.mergeTerms();
		mt.shutDown();
	}

	void mergeTerms() {
		ArrayList<String> terms = new ArrayList<String>();
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb.execute(GET_TRIP_TEXT)) {
			Iterator<String> n_column = result.columnAs("text");
			int i = 0;
			for (String t : IteratorUtil.asIterable(n_column)) {
				terms.add(t);
				i++;
			}
			System.out.println("found " + i + " triplets");
		}
		for (String text : terms) {
			Map<String, Object> params = new HashMap<String, Object>();
			params.put( "txt", text );
			try (Transaction tx = graphDb.beginTx();
					Result result = graphDb.execute(GET_TERMS,params)){
				Iterator<Node> n_column = result.columnAs("t");
				int i = 0;
				Node first=null;
				for (Node node : IteratorUtil.asIterable(n_column)) {	
					if(i==0){
						first=node;
					}else{
						nMerger.merge(first, node);
					}
					i++;
				}
				tx.success();
			} catch (CantMergeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	void createDb() throws IOException {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(
				DB_PATH));
		registerShutdownHook(graphDb);
		nMerger = new NodeMerger(graphDb);
	}

	void shutDown() {
		System.out.println();
		System.out.println("Shutting down database ...");
		// START SNIPPET: shutdownServer
		graphDb.shutdown();
		// END SNIPPET: shutdownServer
	}

	// START SNIPPET: shutdownHook
	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
	// END SNIPPET: shutdownHook

}
