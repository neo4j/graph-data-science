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
package org.neo4j.graphalgo.core.utils.paged.dss;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Disjoint-set-struct is a data structure that keeps track of a set
 * of elements partitioned into a number of disjoint (non-overlapping) subsets.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure">Wiki</a>
 */
public abstract class DisjointSetStruct {

    private final UnionStrategy unionStrategy;

    public DisjointSetStruct(UnionStrategy unionStrategy) {
        this.unionStrategy = unionStrategy;
    }

    /**
     * Joins the set of p (Sp) with set of q (Sq) such that
     * {@link DisjointSetStruct#connected(long, long)}
     * for any pair of (Spi, Sqj) evaluates to true.
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    public final void union(long p, long q) {
        unionStrategy.union(p, q, this);
    }

    /**
     * Find set Id of element p.
     *
     * @param nodeId the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    public abstract long setIdOf(long nodeId);

    abstract long setIdOfRoot(long rootId);

    abstract HugeLongArray parent();

    /**
     * Number of elements stored in the data structure.
     *
     * @return element count
     */
    abstract long capacity();

    /**
     * Find set Id of element p.
     *
     * @param p the set element
     * @return returns the representative member of the set to which p belongs
     */
    abstract long find(long p);

    /**
     * Find set Id of element p without balancing optimization.
     *
     * @param nodeId the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    abstract long findNoOpt(long nodeId);

    /**
     * Merges the given DisjointSetStruct into this one.
     *
     * @param other DisjointSetStruct to merge with
     * @return merged DisjointSetStruct
     */
    public DisjointSetStruct merge(DisjointSetStruct other) {
        if (!getClass().equals(other.getClass())) {
            throw new IllegalArgumentException(String.format(
                    "Expected: %s Actual: %s",
                    getClass().getSimpleName(),
                    other.getClass().getSimpleName()));
        }

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
     * Check if p and q belong to the same set.
     *
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    public final boolean connected(long p, long q) {
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
    final long findPC(long p) {
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
    public final long getSetCount() {
        long capacity = capacity();
        BitSet sets = new BitSet(capacity);
        for (long i = 0L; i < capacity; i++) {
            long setId = find(i);
            sets.set(setId);
        }
        return sets.cardinality();
    }

    /**
     * Compute the size of each set.
     *
     * @return a map which maps setId to setSize
     */
    public final LongLongMap getSetSize() {
        final LongLongMap map = new LongLongScatterMap();
        for (long i = parent().size() - 1; i >= 0; i--) {
            map.addTo(setIdOf(i), 1);
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
    public final Stream<Result> resultStream(IdMapping idMapping) {

        return LongStream.range(IdMapping.START_NODE_ID, idMapping.nodeCount())
                .mapToObj(mappedId ->
                        new Result(
                                idMapping.toOriginalNodeId(mappedId),
                                setIdOf(mappedId)));
    }

    /**
     * Responsible for writing back the set ids to Neo4j.
     */
    public static final class Translator implements PropertyTranslator.OfLong<DisjointSetStruct> {

        public static final PropertyTranslator<DisjointSetStruct> INSTANCE = new Translator();

        @Override
        public long toLong(final DisjointSetStruct data, final long nodeId) {
            return data.setIdOf(nodeId);
        }
    }

    /**
     * Union find result type.
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
}
