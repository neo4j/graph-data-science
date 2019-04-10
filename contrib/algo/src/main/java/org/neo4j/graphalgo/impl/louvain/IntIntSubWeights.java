package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.graphalgo.core.utils.RawValues;

final class IntIntSubWeights extends SubWeights {

  private final LongDoubleScatterMap weights;

  IntIntSubWeights(int nodeCount) {
    weights = new LongDoubleScatterMap(nodeCount);
  }

  void add(final int source, final int target, final double weight) {
    weights.addTo(RawValues.combineIntInt(source, target), weight);
    weights.addTo(RawValues.combineIntInt(target, source), weight);
  }

  @Override
  double getOrDefault(final long source, final long target) {
    return weights.getOrDefault(RawValues.combineIntInt((int) source, (int) target), 0.0);
  }

  @Override
  void release() {
    weights.release();
  }
}
