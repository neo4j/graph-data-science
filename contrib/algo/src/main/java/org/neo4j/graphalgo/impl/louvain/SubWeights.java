package org.neo4j.graphalgo.impl.louvain;

abstract class SubWeights {

  abstract double getOrDefault(long source, long target);

  abstract void release();
}
