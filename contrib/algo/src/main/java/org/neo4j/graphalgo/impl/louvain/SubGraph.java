package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.api.HugeRelationshipConsumer;

abstract class SubGraph {

  abstract void forEach(long nodeId, HugeRelationshipConsumer consumer);

  abstract int degree(long nodeId);

  abstract void release();
}
