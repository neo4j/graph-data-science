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

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Add adaption of the C++ implementation [1] for the
 * "Wait-free Parallel Algorithms for the Union􏰁-Find Problem" [2]
 * with some input from an atomic DSS implementation in Rust [3].
 *
 * The major difference for our DSS is, that we don't supported the
 * Union-by-Rank strategy [3], for techincal and performance reasons.
 *
 * The reference implementation in C++ uses 32bit unsigned integers for
 * both the id values and the rank values. Those two values have to be
 * updated atomically, which [1] does by merging them into a single
 * 64bit unsigned integer and doing atomic/cas operations on that value.
 *
 * We need 64bits for the id value alone and since there is no u128 data type
 * in Java, the only way to update those values would be to use a class for
 * the combination of id+rank and updated the references to that atomically.
 * This is the approach that the Rust implementation is doing, except that
 * Rust allows the struct values to be allocated on the stack and has no GC
 * overhead, where that would not be true for Java (in the near future).
 *
 * We drop the by-Rank functionality and just support Union-by-Min for this DSS.
 *
 * The main difference in implementation comared to the regular DSS is that we
 * use CAS operations to atomically set a set id for some value.
 * We will retry union operations until a thread succeeds in changing the set id
 * for a node. Other threads that might have wanted to write a different value
 * will fail and the CAS operation and redo their union step. This allows for concurrent
 * writes into a single DSS and does not longer necessitate an additional merge step.
 *
 * [1]: https://github.com/wjakob/dset/blob/7967ef0e6041cd9d73b9c7f614ab8ae92e9e587a/dset.h
 * [2]: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.56.8354&rep=rep1&type=pdf
 * [3]: https://github.com/tov/disjoint-sets-rs/blob/88ab08df21f04fcf7c157b6e042efd561ee873ba/src/concurrent.rs
 * [4]: https://en.wikipedia.org/wiki/Disjoint-set_data_structure#by_rank
 */
public final class HugeAtomicDisjointSetStruct implements DisjointSetStruct {

    public static MemoryEstimation memoryEstimation(boolean incremental) {
        MemoryEstimations.Builder builder = MemoryEstimations
                .builder(HugeAtomicDisjointSetStruct.class)
                .perNode("data", HugeAtomicLongArray::memoryEstimation);
        if (incremental) {
            builder.add("internalToProvidedIds", HugeLongLongMap.memoryEstimation());
        }
        return builder.build();
    }

    private final HugeAtomicLongArray data;
    private final HugeLongLongMap internalToProvidedIds;

    public HugeAtomicDisjointSetStruct(long capacity, AllocationTracker tracker) {
        this.data = HugeAtomicLongArray.newArray(capacity, i -> i, tracker);
        this.internalToProvidedIds = null;
    }

    public HugeAtomicDisjointSetStruct(long capacity, HugeWeightMapping communityMapping, AllocationTracker tracker) {
        long maxCommunity = communityMapping.getMaxValue();
        AtomicLong maxCommunityId = new AtomicLong(maxCommunity);
        final HugeLongLongMap internalMapping = new HugeLongLongMap(capacity, tracker);
        this.internalToProvidedIds = new HugeLongLongMap(capacity, tracker);
        this.data = HugeAtomicLongArray.newArray(capacity, nodeId -> {
            long parentValue = nodeId;
            double communityIdValue = communityMapping.nodeWeight(nodeId, Double.NaN);

            if (!Double.isNaN(communityIdValue)) {
                long communityId = (long) communityIdValue;

                long internalCommunityId = internalMapping.getOrDefault(communityId, -1);
                if (internalCommunityId != -1) {
                    parentValue = internalCommunityId;
                } else {
                    internalToProvidedIds.addTo(nodeId, communityId);
                    internalMapping.addTo(communityId, nodeId);
                }
            } else {
                internalToProvidedIds.addTo(nodeId, maxCommunityId.incrementAndGet());
            }
            return parentValue;
        }, tracker);
    }

    private long parent(long id) {
        return data.get(id);
    }

    private long find(long id) {
        long parent;
        while (id != (parent = parent(id))) {
            long grandParent = parent(parent);
            if (parent != grandParent) {
                // Try to apply path-halving by setting the value
                // for some id to its grand parent. This might fail
                // if another thread is also changing the same value
                // but that's ok. The CAS operations guarantees
                // that at least one of the contenting threads will
                // succeed. That's enough for the path-halving to work
                // and there is no need to retry in case of a CAS failure.
                data.compareAndSet(id, parent, grandParent);
            }
            id = grandParent;
        }
        return id;
    }

    @Override
    public long setIdOf(final long nodeId) {
        long setId = find(nodeId);
        if (internalToProvidedIds != null) {
            setId = internalToProvidedIds.getOrDefault(setId, -1L);
            assert setId != -1L;
        }
        return setId;
    }

    @Override
    public boolean sameSet(long id1, long id2) {
        while (true) {
            id1 = find(id1);
            id2 = find(id2);
            if (id1 == id2) {
                return true;
            }
            if (parent(id1) == id1) {
                return false;
            }
        }
    }

    @Override
    public void union(long id1, long id2) {
        while (true) {
            id1 = find(id1);
            id2 = find(id2);

            if (id1 == id2) {
                return;
            }

            // We need to do Union-by-Min, so the smalled ID wins.
            // We also only update the entry for id1 and if that
            // is the smaller value, we need to swap ids so we update
            // only the value for id2, not id1.
            if (id1 < id2) {
                long tmp = id2;
                id2 = id1;
                id1 = tmp;
            }

            long oldEntry = id1;
            long newEntry = id2;

            if (!data.compareAndSet(id1, oldEntry, newEntry)) {
                continue;
            }

            break;
        }
    }

    @Override
    public long size() {
        return data.size();
    }
}
