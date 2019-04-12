/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
