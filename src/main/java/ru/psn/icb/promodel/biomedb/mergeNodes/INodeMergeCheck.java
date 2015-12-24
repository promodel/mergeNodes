package ru.psn.icb.promodel.biomedb.mergeNodes;

import org.neo4j.graphdb.Node;

public interface INodeMergeCheck {
	boolean canMerge(Node n1, Node n2);
}
