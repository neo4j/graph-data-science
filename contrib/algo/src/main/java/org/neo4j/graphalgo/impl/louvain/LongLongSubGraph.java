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
