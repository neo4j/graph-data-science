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


import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;

import java.util.stream.LongStream;

/**
 * Implements {@link DisjointSetStruct} with support for incremental computation based on a previously computed mapping
 * between node ids and set ids.
 * Note that this does not use <a href=https://en.wikipedia.org/wiki/Disjoint-set_data_structure#by_rank">Union by Rank</a>
 * but prefers the minimum set id instead when merging two sets.
 */
public final class IncrementalDisjointSetStruct implements DisjointSetStruct {

    public static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations.builder(IncrementalDisjointSetStruct.class)
            .perNode("parent", HugeLongArray::memoryEstimation)
            .perNode("idCommunityMapping", HugeLongArray::memoryEstimation)
            .build();

    private final HugeLongArray parent;
    private final HugeLongArray idCommunityMapping;
    private final long capacity;
    private final HugeWeightMapping communityMapping;
    private long maxCommunity;

    static MemoryEstimation memoryEstimation() {
        return IncrementalDisjointSetStruct.MEMORY_ESTIMATION;
    }

    /**
     * Initialize the struct with the given capacity.
     * Note: the struct must be {@link IncrementalDisjointSetStruct#reset()} prior use!
     *
     * @param capacity the capacity (maximum node id)
     */
    public IncrementalDisjointSetStruct(long capacity, HugeWeightMapping communityMapping, AllocationTracker tracker) {
        parent = HugeLongArray.newArray(capacity, tracker);
        idCommunityMapping = HugeLongArray.newArray(capacity, tracker);
        this.capacity = capacity;
        this.communityMapping = communityMapping;

        maxCommunity = LongStream
                .range(0, capacity)
                .map(id -> (long) communityMapping.nodeWeight(id, -1))
                .max().orElse(0);
    }

    @Override
    public HugeLongArray parent() {
        return parent;
    }

    /**
     * reset the container
     */
    public IncrementalDisjointSetStruct reset() {
        parent.fill(-1);
        idCommunityMapping.setAll(nodeId -> {
            long communityId = (long) communityMapping.nodeWeight(nodeId, -1);
            return communityId == -1 ? ++maxCommunity : communityId;
        });
        return this;
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
        return findPC(idCommunityMapping.get(p));
    }

    /**
     * join set of p (Sp) with set of q (Sq) so that {@link IncrementalDisjointSetStruct#connected(long, long)}
     * for any pair of (Spi, Sqj) evaluates to true. Some optimizations exists
     * which automatically balance the tree, the "weighted union rule" is used here.
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    public void union(long p, long q) {
        unionSets(find(p), find(q));
    }

    private void unionSets(long pSet, long qSet) {
        if (pSet < qSet) {
            parent.set(qSet, pSet);
        } else if (qSet < pSet) {
            parent.set(pSet, qSet);
        }
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
        while ((np = parent.get(p)) != -1L) {
            p = np;
        }
        return p;
    }

    @Override
    public DisjointSetStruct merge(DisjointSetStruct other) {
        if (!(other instanceof IncrementalDisjointSetStruct)) {
            throw new IllegalArgumentException(String.format(
                    "Expected: %s Actual: %s",
                    getClass().getSimpleName(),
                    other.getClass().getSimpleName()));
        }
        if (other.capacity() != this.capacity()) {
            throw new IllegalArgumentException("Different Capacity");
        }

        for (int nodeId = 0; nodeId < capacity(); nodeId++) {
            long leftSetId = find(nodeId);
            long rightSetId = other.find(nodeId);
            unionSets(leftSetId, rightSetId);
        }

        return this;
    }

}
