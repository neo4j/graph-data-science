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
package org.neo4j.graphalgo.core.utils.dss;

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * disjoint-set-struct is a data structure that keeps track of a set
 * of elements partitioned into a number of disjoint (non-overlapping) subsets.
 * <p>
 * More info:
 * <p>
 * <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure">Wiki</a>
 *
 * @author mknblch
 */
public final class DisjointSetStruct {

    private final int[] parent;
    private final int[] depth;
    private final int capacity;

    /**
     * Initialize the struct with the given capacity.
     * Note: the struct must be {@link DisjointSetStruct#reset()} prior use!
     *
     * @param capacity the capacity (maximum node id)
     */
    public DisjointSetStruct(int capacity) {
        parent = new int[capacity];
        depth = new int[capacity];
        this.capacity = capacity;
    }

    public DisjointSetStruct merge(DisjointSetStruct other) {

        if (other.capacity != this.capacity) {
            throw new IllegalArgumentException("Different Capacity");
        }

        for (int i = other.parent.length - 1; i >= 0; i--) {
            if (other.parent[i] == -1) {
                continue;
            }
            union(i, other.find(i));
        }

        return this;
    }

    /**
     * reset the container
     */
    public DisjointSetStruct reset() {
        Arrays.fill(parent, -1);
        Arrays.fill(depth, 0);
        return this;
    }

    /**
     * iterate each node and find its setId
     *
     * @param consumer the consumer
     */
    public void forEach(Consumer consumer) {
        for (int i = parent.length - 1; i >= 0; i--) {
            if (!consumer.consume(i, find(i))) {
                break;
            }
        }
    }

    /**
     * @return return a Iterator for each nodeIt-setId combination
     */
    public Iterator<Cursor> iterator() {
        return new NodeSetIterator(this);
    }

    /**
     * @param start  startNodeId
     * @param length number of nodes to process
     * @return return an Iterator over each nodeIt-setId combination within its bounds
     */
    public Iterator<Cursor> iterator(int start, int length) {
        return new ConcurrentNodeSetIterator(this, start, length);
    }

    public Stream<Result> resultStream(IdMapping idMapping) {

        return IntStream.range(IntIdMap.START_NODE_ID, (int) idMapping.nodeCount())
                .mapToObj(mappedId ->
                        new Result(
                                idMapping.toOriginalNodeId(mappedId),
                                find(mappedId)));
    }

    /**
     * element (node) count
     *
     * @return the element count
     */
    public int capacity() {
        return parent.length;
    }


    /**
     * find setId of element p.
     *
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public int find(int p) {
        return findPC(p);
    }

    /**
     * find setId of element p without balancing optimization.
     *
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public int findNoOpt(int p) {
        while (-1 != parent[p]) {
            p = parent[p];
        }
        return p;
    }

    /**
     * find setId of element p.
     * <p>
     * find-impl using a recursive path compression logic
     *
     * @param p the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public int findPC(int p) {
        if (parent[p] == -1) return p;
        // path compression optimization
        parent[p] = findPC(parent[p]); // balance tree while traversing
        return parent[p];
    }

    /**
     * check if p and q belong to the same set
     *
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    public boolean connected(int p, int q) {
        return find(p) == find(q);
    }


    /**
     * join set of p (Sp) with set of q (Sq) so that {@link DisjointSetStruct#connected(int, int)}
     * for any pair of (Spi, Sqj) evaluates to true. Some optimizations exists
     * which automatically balance the tree, the "weighted union rule" is used here.
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    public void union(int p, int q) {
        final int pSet = find(p);
        final int qSet = find(q);
        if (pSet == qSet) {
            return;
        }
        // weighted union rule optimization
        int dq = depth[qSet];
        int dp = depth[pSet];
        if (dp < dq) {
            // attach the smaller tree to the root of the bigger tree
            parent[pSet] = qSet;
        } else if (dp > dq) {
            parent[qSet] = pSet;
        } else {
            parent[qSet] = pSet;
            depth[pSet] += depth[qSet] + 1;
        }
    }

    /**
     * evaluate number of sets
     * @return
     */
    public int getSetCount() {
        final IntSet set = new IntScatterSet();
        forEach((nodeId, setId) -> {
            set.add(setId);
            return true;
        });
        return set.size();
    }

    /**
     * evaluate the size of each set.
     *
     * @return a map which maps setId to setSize
     */
    public IntIntMap getSetSize() {
        final IntIntScatterMap map = new IntIntScatterMap();
        for (int i = parent.length - 1; i >= 0; i--) {
            map.addTo(find(i), 1);
        }
        return map;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("\n");
        for (int i = 0; i < capacity(); i++) {
            builder.append(String.format(" %d ", i));
        }
        builder.append("\n");
        for (int i = 0; i < capacity(); i++) {
            builder.append(String.format("[%d]", find(i)));
        }
        return builder.toString();
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
        boolean consume(int nodeId, int setId);
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

    /**
     * Iterator only usable for single threaded evaluation.
     * Does tree-balancing during iteration.
     */
    private static class NodeSetIterator implements Iterator<Cursor> {

        private final Cursor cursor = new Cursor();
        private final DisjointSetStruct struct;
        private final int length;

//        private int offset = IntIdMap.START_NODE_ID;
        private int offset = IntIdMap.START_NODE_ID;

        private NodeSetIterator(DisjointSetStruct struct) {
            this.struct = struct;
            this.length = struct.capacity();
        }

        @Override
        public boolean hasNext() {
            return offset < length;
        }

        @Override
        public Cursor next() {
            cursor.nodeId = offset;
            cursor.setId = struct.find(offset);
            return cursor;
        }
    }

    /**
     * Iterator usable in multithreaded environment
     */
    private static class ConcurrentNodeSetIterator implements Iterator<Cursor> {

        private final Cursor cursor = new Cursor();
        private final DisjointSetStruct struct;
        private final int length;

        private int offset = 0;

        private ConcurrentNodeSetIterator(DisjointSetStruct struct, int startOffset, int length) {
            this.struct = struct;
            this.length = length + offset > struct.capacity()
                    ? struct.capacity() - offset
                    : length;
            this.offset = startOffset;
        }

        @Override
        public boolean hasNext() {
            return offset < length;
        }

        @Override
        public Cursor next() {
            cursor.nodeId = offset;
            cursor.setId = struct.findNoOpt(offset);
            return cursor;
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

    /**
     * concurrent property translator for export
     */
    public final static class Translator implements PropertyTranslator.OfInt<DisjointSetStruct> {

        public static final PropertyTranslator<DisjointSetStruct> INSTANCE = new Translator();

        @Override
        public int toInt(final DisjointSetStruct data, final long nodeId) {
            return data.findNoOpt((int) nodeId);
        }
    }

}
