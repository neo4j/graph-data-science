/*
 * Copyright (c) 2017-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl.spanningTrees;

import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

/**
 * group of nodes that form a spanning tree
 */
public class SpanningTree {

    public final int head;
    public final int nodeCount;
    public final int effectiveNodeCount;
    public final int[] parent;

    public SpanningTree(int head, int nodeCount, int effectiveNodeCount, int[] parent) {
        this.head = head;
        this.nodeCount = nodeCount;
        this.effectiveNodeCount = effectiveNodeCount;
        this.parent = parent;
    }

    public void forEach(RelationshipConsumer consumer) {
        for (int i = 0; i < nodeCount; i++) {
            final int parent = this.parent[i];
            if (parent == -1) {
                continue;
            }
            if (!consumer.accept(parent, i)) {
                return;
            }
        }
    }

    public int head(int node) {
        int p = node;
        while (-1 != parent[p]) {
            p = parent[p];
        }
        return p;
    }

    public static final PropertyTranslator<SpanningTree> TRANSLATOR = new SpanningTreeTranslator();

    public static class SpanningTreeTranslator implements PropertyTranslator.OfInt<SpanningTree> {
        @Override
        public int toInt(final SpanningTree data, final long nodeId) {
            return data.head((int) nodeId);
        }
    }

}