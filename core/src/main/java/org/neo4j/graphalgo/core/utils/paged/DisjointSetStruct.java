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
package org.neo4j.graphalgo.core.utils.paged;


import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;
import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * disjoint-set-struct is a data structure that keeps track of a set
 * of elements partitioned into a number of disjoint (non-overlapping) subsets.
 * <p>
 * More info:
 * <p>
 * <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure">Wiki</a>
 */
public final class DisjointSetStruct {

    public static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations.builder(DisjointSetStruct.class)
            .perNode("parent", HugeLongArray::memoryEstimation)
            .perNode("depth", HugeLongArray::memoryEstimation)
            .build();

    private final HugeLongArray parent;
    private final HugeLongArray depth;
    private final long capacity;

    /**
     * Initialize the struct with the given capacity.
     * Note: the struct must be {@link DisjointSetStruct#reset()} prior use!
     *
     * @param capacity the capacity (maximum node id)
     */
    public DisjointSetStruct(long capacity, AllocationTracker tracker) {
        parent = HugeLongArray.newArray(capacity, tracker);
        depth = HugeLongArray.newArray(capacity, tracker);
        this.capacity = capacity;
    }

    /**
     * reset the container
     */
    public DisjointSetStruct reset() {
        parent.fill(-1);
        return this;
    }

    public static MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    /**
     * element (node) count
     *
     * @return the element count
     */
    public long capacity() {
        return capacity;
    }

    /**
     * check if p and q belong to the same set
     *
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    public boolean connected(long p, long q) {
        return find(p) == find(q);
    }

    /**
     * find setId of element p.
     *
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public long find(long p) {
        return findPC(p);
    }

    /**
     * find setId of element p.
     * <p>
     * find-impl using a recursive path compression logic
     *
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    private long findPC(long p) {
        long pv = parent.get(p);
        if (pv == -1L) {
            return p;
        }
        // path compression optimization
        // TODO
        long value = find(pv);
        parent.set(p, value);
        return value;
    }

    /**
     * join set of p (Sp) with set of q (Sq) so that {@link DisjointSetStruct#connected(long, long)}
     * for any pair of (Spi, Sqj) evaluates to true. Some optimizations exists
     * which automatically balance the tree, the "weighted union rule" is used here.
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    public void union(long p, long q) {
        final long pSet = find(p);
        final long qSet = find(q);
        if (pSet == qSet) {
            return;
        }
        // weighted union rule optimization
        long dq = depth.get(qSet);
        long dp = depth.get(pSet);
        if (dp < dq) {
            // attach the smaller tree to the root of the bigger tree
            parent.set(pSet, qSet);
        } else if (dp > dq) {
            parent.set(qSet, pSet);
        } else {
            parent.set(qSet, pSet);
            depth.addTo(pSet, dq + 1);
        }
    }

    public DisjointSetStruct merge(DisjointSetStruct other) {

        if (other.capacity != this.capacity) {
            throw new IllegalArgumentException("Different Capacity");
        }

        final HugeCursor<long[]> others = other.parent.cursor(other.parent.newCursor());
        long i = 0L;
        while (others.next()) {
            long[] array = others.array;
            int offset = others.offset;
            int limit = others.limit;
            while (offset < limit) {
                if (array[offset++] != -1L) {
                    union(i, other.find(i));
                }
                ++i;
            }
        }

        return this;
    }

    /**
     * find setId of element p without balancing optimization.
     *
     * @param nodeId the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public long findNoOpt(final long nodeId) {
        long p = nodeId;
        long np;
        while ((np = parent.get(p)) != -1) {
            p = np;
        }
        return p;
    }

    /**
     * evaluate number of sets
     *
     * @return
     */
    public int getSetCount() {
        LongScatterSet set = new LongScatterSet();
        for (long i = 0L; i < capacity; ++i) {
            long setId = find(i);
            set.add(setId);
        }
        return set.size();
    }

    /**
     * evaluate the size of each set.
     *
     * @return a map which maps setId to setSize
     */
    public LongLongMap getSetSize() {
        final LongLongScatterMap map = new LongLongScatterMap();

        for (long i = parent.size() - 1; i >= 0; i--) {
            map.addTo(find(i), 1);
        }
        return map;
    }

    public Stream<Result> resultStream(IdMapping idMapping) {

        return LongStream.range(IdMapping.START_NODE_ID, idMapping.nodeCount())
                .mapToObj(mappedId ->
                        new Result(
                                idMapping.toOriginalNodeId(mappedId),
                                find(mappedId)));
    }

    /**
     * iterate each node and find its setId
     *
     * @param consumer the consumer
     */
    public void forEach(DisjointSetStruct.Consumer consumer) {
        for (long i = parent.size() - 1; i >= 0; i--) {
            if (!consumer.consume(i, find(i))) {
                break;
            }
        }
    }

    /**
     * Consumer interface for c
     */
    @FunctionalInterface
    public interface Consumer {
        /**
         * @param nodeId the mapped node id
         * @param setId  the set id where the node belongs to
         * @return true to continue the iteration, false to stop
         */
        boolean consume(long nodeId, long setId);
    }

    public static final class Translator implements PropertyTranslator.OfLong<DisjointSetStruct> {

        public static final PropertyTranslator<DisjointSetStruct> INSTANCE = new Translator();

        @Override
        public long toLong(final DisjointSetStruct data, final long nodeId) {
            return data.findNoOpt(nodeId);
        }
    }

    /**
     * union find result type
     */
    public static class Result {

        /**
         * the mapped node id
         */
        public final long nodeId;

        /**
         * set id
         */
        public final long setId;

        public Result(long nodeId, int setId) {
            this.nodeId = nodeId;
            this.setId = (long) setId;
        }

        public Result(long nodeId, long setId) {
            this.nodeId = nodeId;
            this.setId = setId;
        }
    }

    public static class Cursor {
        /**
         * the mapped node id
         */
        int nodeId;
        /**
         * the set id of the node
         */
        int setId;
    }
}
