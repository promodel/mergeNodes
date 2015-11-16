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

public class MergeXRefs {
	public static String DB_PATH = "/Users/lptolik/Documents/Neo4j/data/graph.db";
	public static String GET_DB = "match (d:DB) return d.name";
	public static String GET_XREF_ID = "match (x1:XRef)-[:LINK_TO]->(d:DB)<-[:LINK_TO]-(x2:XRef) "
			+ "where d.name={dbn} and "
			+ "x1.id=x2.id and "
			+ "id(x1)<id(x2) "
			+ "return x1,x2 limit 25";
	public static String GET_XREFS = "match (t:Term) where t.text={txt} return t order by id(t)";

	private GraphDatabaseService graphDb;

	private NodeMerger nMerger;

	public static void main(String[] args) throws IOException {
		MergeXRefs mt = new MergeXRefs();
		mt.createDb();
		mt.mergeXRefs();
		mt.shutDown();
	}

	void mergeXRefs() {
		ArrayList<String> dbs = new ArrayList<String>();
		try (Transaction tx = graphDb.beginTx();
				Result result = graphDb.execute(GET_DB)) {
			Iterator<String> n_column = result.columnAs("d.name");
			int i = 0;
			for (String t : IteratorUtil.asIterable(n_column)) {
				dbs.add(t);
				i++;
			}
			System.out.println("found " + i + " databases");
		}
		for (String text : dbs) {
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("dbn", text);
			int tot = 0;
			int i = 25;
			while (i == 25) {
				i = 0;
				try (Transaction tx = graphDb.beginTx();
						Result result = graphDb.execute(GET_XREF_ID, params)) {
					while (result.hasNext()) {
						Map<String, Object> row = result.next();
						Node first = (Node) row.get("x1");
						Node second = (Node) row.get("x2");
						nMerger.merge(first, second);
						i++;
					}
					tx.success();
				} catch (CantMergeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				tot += i;
			}
			System.out.println("For db " + text + " merged " + tot + " xrefs");
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
