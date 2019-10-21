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
package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.IntFloatHashMap;
import com.carrotsearch.hppc.cursors.IntFloatCursor;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;

final class IntIntSubGraph extends SubGraph {

    private IntFloatHashMap[] graph;
    private final int communityCount;

    IntIntSubGraph(final int communityCount, boolean hasRelationshipProperty) {
        super(hasRelationshipProperty);
        this.graph = new IntFloatHashMap[communityCount];
        this.communityCount = communityCount;
    }

    void add(final int source, final int target, final float weight) {
        assert source < graph.length && target < graph.length;
        putIfAbsent(source).addTo(target, weight);
        putIfAbsent(target).addTo(source, weight);
    }

    @Override
    public long nodeCount() {
        return communityCount;
    }

    @Override
    void forEach(final long nodeId, final RelationshipWithPropertyConsumer consumer) {
        assert nodeId < graph.length;
        IntFloatHashMap targets = graph[(int) nodeId];
        if (targets != null) {
            for (IntFloatCursor cursor : targets) {
                if (!consumer.accept(nodeId, cursor.key, cursor.value)) {
                    return;
                }
            }
        }
    }

    @Override
    int degree(final long nodeId) {
        assert nodeId < graph.length;
        IntFloatHashMap targets = graph[(int) nodeId];
        return null == targets ? 0 : targets.size();
    }

    @Override
    public void release() {
        graph = null;
    }

    private IntFloatHashMap putIfAbsent(final int communityId) {
        IntFloatHashMap targets = graph[communityId];
        if (null == targets) {
            targets = new IntFloatHashMap();
            graph[communityId] = targets;
        }
        return targets;
    }
}
