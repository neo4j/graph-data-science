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

import com.carrotsearch.hppc.LongFloatHashMap;
import com.carrotsearch.hppc.cursors.LongFloatCursor;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

final class LongLongSubGraph extends SubGraph {

    private HugeObjectArray<LongFloatHashMap> graph;
    private final long nodeCount;
    private final AllocationTracker tracker;

    LongLongSubGraph(final long nodeCount, boolean hasRelationshipProperty, final AllocationTracker tracker) {
        super(hasRelationshipProperty);
        this.nodeCount = nodeCount;
        this.tracker = tracker;
        graph = HugeObjectArray.newArray(LongFloatHashMap.class, nodeCount, tracker);
    }

    void add(final long source, final long target, final float weight) {
        assert source < graph.size() && target < graph.size();
        graph.putIfAbsent(source, LongFloatHashMap::new).addTo(target, weight);
        graph.putIfAbsent(target, LongFloatHashMap::new).addTo(source, weight);
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    void forEach(final long nodeId, final WeightedRelationshipConsumer consumer) {
        LongFloatHashMap targets = graph.get(nodeId);
        if (targets != null) {
            for (LongFloatCursor cursor : targets) {
                if (!consumer.accept(nodeId, cursor.key, cursor.value)) {
                    return;
                }
            }
        }
    }

    @Override
    int degree(final long nodeId) {
        assert nodeId < graph.size();
        LongFloatHashMap targets = graph.get(nodeId);
        return null == targets ? 0 : targets.size();
    }

    @Override
    public void release() {
        tracker.remove(graph.release());
        graph = null;
    }
}
