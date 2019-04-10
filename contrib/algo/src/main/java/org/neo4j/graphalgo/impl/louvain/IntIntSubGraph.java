package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;

final class IntIntSubGraph extends SubGraph {

  private final IntObjectHashMap<IntScatterSet> graph;

  IntIntSubGraph(int nodeCount) {
    this.graph = new IntObjectScatterMap<>(nodeCount);
  }

  void add(int source, int target) {
    Louvain.putIfAbsent(graph, source).add(target);
    Louvain.putIfAbsent(graph, target).add(source);
  }

  @Override
  void forEach(final long nodeId, final HugeRelationshipConsumer consumer) {
    final IntContainer rels = graph.get((int) nodeId);
    if (null == rels) {
      return;
    }
    for (IntCursor cursor : rels) {
      if (!consumer.accept(nodeId, (long) cursor.value)) {
        return;
      }
    }
  }

  @Override
  int degree(final long nodeId) {
    final IntContainer rels = graph.get((int) nodeId);
    if (null == rels) {
      return 0;
    }
    return rels.size();
  }

  @Override
  void release() {
    graph.release();
  }
}
