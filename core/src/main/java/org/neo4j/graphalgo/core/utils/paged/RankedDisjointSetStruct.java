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


import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;

/**
 * Implementation of {@link DisjointSetStruct} uses Union by Rank and Path compression.
 */
public class RankedDisjointSetStruct implements DisjointSetStruct {

    public static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations.builder(RankedDisjointSetStruct.class)
            .perNode("parent", HugeLongArray::memoryEstimation)
            .perNode("depth", HugeLongArray::memoryEstimation)
            .build();

    private final HugeLongArray parent;
    private final HugeLongArray depth;
    private final long capacity;

    public static MemoryEstimation memoryEstimation() {
        return RankedDisjointSetStruct.MEMORY_ESTIMATION;
    }

    /**
     * Initialize the struct with the given capacity.
     * Note: the struct must be {@link RankedDisjointSetStruct#reset()} prior use!
     *
     * @param capacity the capacity (maximum node id)
     */
    public RankedDisjointSetStruct(long capacity, AllocationTracker tracker) {
        parent = HugeLongArray.newArray(capacity, tracker);
        depth = HugeLongArray.newArray(capacity, tracker);
        this.capacity = capacity;
    }

    @Override
    public HugeLongArray parent() {
        return parent;
    }

    @Override
    public RankedDisjointSetStruct reset() {
        parent.fill(-1L);
        return this;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public long find(long p) {
        return findPC(p);
    }

    /**
     * {@inheritDoc}
     *
     * Uses <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure#by_rank%22&gt;Rank&lt;/a&gt;">rank based tree balancing.</a>
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    @Override
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

    @Override
    public RankedDisjointSetStruct merge(DisjointSetStruct other) {

        if (!(other instanceof RankedDisjointSetStruct)) {
            throw new IllegalArgumentException(String.format(
                    "Cannot merge RankedDisjointSetStruct with %s",
                    other.getClass().getSimpleName()));
        }

        if (other.capacity() != this.capacity) {
            throw new IllegalArgumentException("Different Capacity");
        }

        final HugeCursor<long[]> others = other.parent().cursor(other.parent().newCursor());
        long i = 0L;
        while (others.next()) {
            long[] parentPage = others.array;
            int offset = others.offset;
            int limit = others.limit;
            while (offset < limit) {
                // Skip root nodes
                if (parentPage[offset] != -1L) {
                    union(i, other.find(i));
                }
                ++offset;
                ++i;
            }
        }

        return this;
    }

    @Override
    public long findNoOpt(final long nodeId) {
        long p = nodeId;
        long np;
        while ((np = parent.get(p)) != -1L) {
            p = np;
        }
        return p;
    }
}
