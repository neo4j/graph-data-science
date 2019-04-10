package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongDoubleMap;

final class LongLongSubWeights extends SubWeights {

  private final HugeLongLongDoubleMap weights;

  LongLongSubWeights(long nodeCount, AllocationTracker tracker) {
    weights = new HugeLongLongDoubleMap(nodeCount, tracker);
  }

  void add(long source, long target, double weight) {
    weights.addTo(source, target, weight);
    weights.addTo(target, source, weight);
  }

  @Override
  double getOrDefault(long source, long target) {
    return weights.getOrDefault(source, target, 0.0);
  }

  @Override
  void release() {
    weights.release();
  }
}
