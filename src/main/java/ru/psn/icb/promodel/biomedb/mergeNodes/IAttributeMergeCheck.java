package ru.psn.icb.promodel.biomedb.mergeNodes;

import org.neo4j.graphdb.PropertyContainer;

public interface IAttributeMergeCheck {
	boolean canMerge(PropertyContainer n1, PropertyContainer n2,String attribute);
}
