package ru.psn.icb.promodel.biomedb.mergeNodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class NodeMerger implements INodeMergeCheck, IEdgeMergeCheck,
		IAttributeMergeCheck {
	private final GraphDatabaseService graphDb;
	private INodeMergeCheck ncheck;
	private IEdgeMergeCheck echeck;
	private IAttributeMergeCheck acheck;

	public NodeMerger(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
		ncheck = this;
		echeck = this;
		acheck = this;
	}

	public void merge(Node toKeep, Node toRemove) {
		try (Transaction tx = graphDb.beginTx()) {
			// TODO check that both nodes are in the database
			checkNodeInDb(toKeep);
			checkNodeInDb(toRemove);
			// TODO check that nodes can be merged
			if (ncheck.canMerge(toKeep, toRemove)) {
				// TODO find all edges for toRemove and clone them
				for (Relationship r : toRemove.getRelationships()) {
					Node on = r.getOtherNode(toRemove);
					// TODO remove all edges of toRemove
					toKeep.createRelationshipTo(on, r.getType());
					r.delete();
				}
				// TODO remove toRemove
				toRemove.delete();
			}
			tx.success();
		}
	}

	void checkNodeInDb(Node n) {
		if (!n.getGraphDatabase().equals(graphDb))
			throw new IllegalArgumentException(
					"First node does not belong to the database at hand");
	}

	Map<Node, List<Relationship>> getEdges(Node n) {
		Map<Node, List<Relationship>> res = new HashMap<Node, List<Relationship>>();
		try (Transaction tx = graphDb.beginTx()) {
			checkNodeInDb(n);
			for (Relationship r : n.getRelationships()) {
				Node on = r.getOtherNode(n);
				if (!res.containsKey(on)) {
					res.put(on, new ArrayList<Relationship>());
				}
				res.get(on).add(r);
			}
			tx.success();
		}
		return res;
	}

	boolean isNeighbours(Node n1, Node n2) {
		return false;
	}

	void mergeEdges(Node toKeep, Node toRemove, Relationship toMerge) {

	}

	@Override
	public boolean canMerge(PropertyContainer n1, PropertyContainer n2) {
		return true;
	}

	@Override
	public boolean canMerge(Relationship r1, Relationship r2) {
		return true;
	}

	@Override
	public boolean canMerge(Node n1, Node n2) {
		return true;
	}

}
