package ru.psn.icb.promodel.biomedb.mergeNodes;

import org.neo4j.graphdb.Relationship;

public interface IEdgeMergeCheck {
	boolean canMerge(Relationship r1, Relationship r2);

	boolean isParallesAllowed(Relationship toMerge);

}
