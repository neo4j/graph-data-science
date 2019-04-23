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

import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

final class LongLongSubGraph extends SubGraph {

    private HugeObjectArray<Roaring64NavigableMap> nodes;
    private final AllocationTracker tracker;

    LongLongSubGraph(long nodeCount, AllocationTracker tracker) {
        this.tracker = tracker;
        nodes = HugeObjectArray.newArray(Roaring64NavigableMap.class, nodeCount, tracker);
    }

    void add(long source, long target) {
        nodes.putIfAbsent(source, Roaring64NavigableMap::new).addLong(target);
        nodes.putIfAbsent(target, Roaring64NavigableMap::new).addLong(source);
    }

    @Override
    void forEach(final long nodeId, final HugeRelationshipConsumer consumer) {
        Roaring64NavigableMap map = nodes.get(nodeId);
        if (map == null) {
            return;
        }
        LongIterator ints = map.getLongIterator();
        while (ints.hasNext()) {
            if (!consumer.accept(nodeId, ints.next())) {
                return;
            }
        }
    }

    @Override
    int degree(long nodeId) {
        Roaring64NavigableMap map = nodes.get(nodeId);
        if (map == null) {
            return 0;
        }
        return map.getIntCardinality();
    }

    @Override
    void release() {
        tracker.remove(nodes.release());
        nodes = null;
    }
}
