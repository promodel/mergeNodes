package ru.psn.icb.promodel.biomedb.mergeNodes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

/**
 * Basic class for data merging in the database. All elements of the class supposed to be invoked within transaction. 
 * 
 *  Class implements simplest versions of Check interfaces that just verify labels and always allow to merge attributes.
 *  
 * @author lptolik
 *
 */
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

	/**
	 * Main method to merge nodes. It suppose to run within transaction.
	 * @param toKeep node, which data to be merged in, and going to be preserved in the database.
	 * @param toRemove node, which data to be taken from, and supposed to be removed.
	 * @throws CantMergeException 
	 */
	public void merge(Node toKeep, Node toRemove) throws CantMergeException {
//		try (Transaction tx = graphDb.beginTx()) {
			// TODO check that both nodes are in the database
			checkNodeInDb(toKeep);
			checkNodeInDb(toRemove);
			// TODO check that nodes can be merged
			if (ncheck.canMerge(toKeep, toRemove)) {
				// TODO find all edges for toRemove and clone them
				for (Relationship r : toRemove.getRelationships()) {
					try {
						mergeEdges(toKeep, toRemove, r);
					} catch (CantMergeException e) {
						throw new CantMergeException("Nodes "+toRemove.toString()+" and "+toKeep.toString()+" cannot be merged",e);
					}
				}
				// TODO remove toRemove
				toRemove.delete();
			}
//			tx.success();
//		}
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

	void mergeEdges(Node toKeep, Node toRemove, Relationship toMerge) throws CantMergeException {
		Node on = toMerge.getOtherNode(toRemove);
		// TODO remove all edges of toRemove
		Relationship parallel=null;
		for (Relationship r :toKeep.getRelationships(toMerge.getType())){
			if(r.getOtherNode(toKeep).equals(on)){
				//Check direction
				if(r.getStartNode().equals(toMerge.getStartNode()) || 
						r.getEndNode().equals(toMerge.getEndNode())){
					parallel=r;
					break;
				}
			}
		}
		if(parallel==null){
		toKeep.createRelationshipTo(on, toMerge.getType());
		}else if(canMerge(toMerge, parallel)){
			
		}else{
			throw new CantMergeException("Edges "+toMerge.toString()+" and "+parallel.toString()+" cannot be merged");
		}
		toMerge.delete();

	}

	@Override
	public boolean canMerge(PropertyContainer n1, PropertyContainer n2) {
		return true;
	}

	@Override
	public boolean canMerge(Relationship r1, Relationship r2) {
		boolean res=false;
		res=r1.isType(r2.getType());
		return res;
	}

	@Override
	public boolean canMerge(Node n1, Node n2) {
		boolean res=false;
		for(Label l:n1.getLabels()){
			res=res||n2.hasLabel(l);
		}
		return res;
	}

	public INodeMergeCheck getNcheck() {
		return ncheck;
	}

	public void setNcheck(INodeMergeCheck ncheck) {
		this.ncheck = ncheck;
	}

	public IEdgeMergeCheck getEcheck() {
		return echeck;
	}

	public void setEcheck(IEdgeMergeCheck echeck) {
		this.echeck = echeck;
	}

	public IAttributeMergeCheck getAcheck() {
		return acheck;
	}

	public void setAcheck(IAttributeMergeCheck acheck) {
		this.acheck = acheck;
	}

}
