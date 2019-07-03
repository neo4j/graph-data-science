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
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Disjoint-set-struct is a data structure that keeps track of a set
 * of elements partitioned into a number of disjoint (non-overlapping) subsets.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure">Wiki</a>
 */
public interface DisjointSetStruct {

    HugeLongArray parent();

    /**
     * Initializes the data structure.
     */
    DisjointSetStruct reset();

    /**
     * Number of elements stored in the data structure.
     *
     * @return element count
     */
    long capacity();

    /**
     * Find set Id of element p.
     *
     * @param p the set element
     * @return returns the representative member of the set to which p belongs
     */
    long find(long p);

    /**
     * Joins the set of p (Sp) with set of q (Sq) such that
     * {@link DisjointSetStruct#connected(long, long)}
     * for any pair of (Spi, Sqj) evaluates to true.
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    void union(long p, long q);

    /**
     * Merges the given DisjointSetStruct into this one.
     *
     * @param other DisjointSetStruct to merge with
     * @return merged DisjointSetStruct
     */
    default DisjointSetStruct merge(DisjointSetStruct other) {
        if (other.capacity() != this.capacity()) {
            throw new IllegalArgumentException("Different Capacity");
        }

        final HugeCursor<long[]> others = other.parent().initCursor(other.parent().newCursor());
        long nodeId = 0L;
        while (others.next()) {
            long[] parentPage = others.array;
            int offset = others.offset;
            int limit = others.limit;
            while (offset < limit) {
                // Skip root nodes
                if (parentPage[offset] != -1L) {
                    union(nodeId, other.find(nodeId));
                }
                ++offset;
                ++nodeId;
            }
        }

        return this;
    }

    /**
     * Find set Id of element p without balancing optimization.
     *
     * @param nodeId the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    long findNoOpt(long nodeId);

    /**
     * Check if p and q belong to the same set.
     *
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    default boolean connected(long p, long q) {
        return find(p) == find(q);
    }

    /**
     * Find set Id of element p.
     * <p>
     * This method uses recursive path compression logic.
     *
     * @param p the set element
     * @return returns the representative member of the set to which p belongs
     */
    default long findPC(long p) {
        long pv = parent().get(p);
        if (pv == -1L) {
            return p;
        }
        // path compression optimization
        long value = findPC(pv);
        parent().set(p, value);
        return value;
    }

    /**
     * Compute number of sets present.
     *
     * @note This is very expensive.
     */
    default int getSetCount() {
        LongScatterSet set = new LongScatterSet();
        for (long i = 0L; i < capacity(); ++i) {
            long setId = find(i);
            set.add(setId);
        }
        return set.size();
    }

    /**
     * Compute the size of each set.
     *
     * @return a map which maps setId to setSize
     */
    default LongLongMap getSetSize() {
        final LongLongScatterMap map = new LongLongScatterMap();

        for (long i = parent().size() - 1; i >= 0; i--) {
            map.addTo(find(i), 1);
        }
        return map;
    }

    /**
     * Computes the result stream based on a given ID mapping by using
     * {@link #find(long)} to look up the set representative for each node id.
     *
     * @param idMapping mapping between internal ids and Neo4j ids
     * @return tuples of Neo4j ids and their set ids
     */
    default Stream<Result> resultStream(IdMapping idMapping) {

        return LongStream.range(IdMapping.START_NODE_ID, idMapping.nodeCount())
                .mapToObj(mappedId ->
                        new Result(
                                idMapping.toOriginalNodeId(mappedId),
                                find(mappedId)));
    }

    /**
     * Iterate each node and find its setId.
     *
     * @param consumer the consumer
     */
    default void forEach(Consumer consumer) {
        for (long i = parent().size() - 1; i >= 0; i--) {
            if (!consumer.consume(i, find(i))) {
                break;
            }
        }
    }

    /**
     * Consumer interface for {@link #forEach(Consumer)}.
     */
    @FunctionalInterface
    interface Consumer {
        /**
         * @param nodeId the mapped node id
         * @param setId  the set id where the node belongs to
         * @return true to continue the iteration, false to stop
         */
        boolean consume(long nodeId, long setId);
    }

    /**
     * Responsible for writing back the set ids to Neo4j.
     */
    final class Translator implements PropertyTranslator.OfLong<DisjointSetStruct> {

        public static final PropertyTranslator<DisjointSetStruct> INSTANCE = new Translator();

        @Override
        public long toLong(final DisjointSetStruct data, final long nodeId) {
            return data.findNoOpt(nodeId);
        }
    }

    /**
     * Union find result type.
     */
    class Result {

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
}
