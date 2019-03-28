package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

final class SubGraph {

    private HugeObjectArray<Roaring64NavigableMap> nodes;
    private final AllocationTracker tracker;

    SubGraph(long nodeCount, AllocationTracker tracker) {
        this.tracker = tracker;
        nodes = HugeObjectArray.newArray(Roaring64NavigableMap.class, nodeCount, tracker);
    }

    void add(long source, long target) {
        nodes.putIfAbsent(source, Roaring64NavigableMap::new).addLong(target);
    }

    LongIterator get(long nodeId) {
        Roaring64NavigableMap map = nodes.get(nodeId);
        return map != null ? map.getLongIterator() : null;
    }

    int degree(long nodeId) {
        Roaring64NavigableMap map = nodes.get(nodeId);
        if (map == null) {
            return 0;
        }
        return map.getIntCardinality();
    }

    void release() {
        tracker.remove(nodes.release());
        nodes = null;
    }
}
