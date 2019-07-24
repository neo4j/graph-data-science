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

    // mutable std::vector<std::atomic<uint64_t>> mData;
    private final HugeAtomicLongArray data;
    private final HugeLongLongMap internalToProvidedIds;

    // DisjointSets(uint32_t size) : mData(size) {
    //    for (uint32_t i=0; i<size; ++i)
    //        mData[i] = (uint32_t) i;
    //}
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

    // uint32_t parent(uint32_t id) const {
    //        return (uint32_t) mData[id];
    //    }

    private long parent(long id) {
        return data.get(id);
    }

    // uint32_t find(uint32_t id) const {
    //    while (id != parent(id)) {
    //        uint64_t value = mData[id];
    //        uint32_t new_parent = parent((uint32_t) value);
    //        uint64_t new_value =
    //            (value & 0xFFFFFFFF00000000ULL) | new_parent;
    //        /* Try to update parent (may fail, that's ok) */
    //        if (value != new_value)
    //            mData[id].compare_exchange_weak(value, new_value);
    //        id = new_parent;
    //    }
    //    return id;
    //}

    private long find(long id) {
        while (id != parent(id)) {
            long value = data.get(id);
            long newParent = parent(value);
            if (value != newParent) {
                /* Try to update parent (may fail, that's ok) */
                data.compareAndSet(id, value, newParent);
            }
            id = newParent;
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

    // bool same(uint32_t id1, uint32_t id2) const {
    //    for (;;) {
    //        id1 = find(id1);
    //        id2 = find(id2);
    //        if (id1 == id2)
    //            return true;
    //        if (parent(id1) == id1)
    //            return false;
    //    }
    //}

    @Override
    public boolean connected(long id1, long id2) {
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

    // uint32_t unite(uint32_t id1, uint32_t id2) {
    //    for (;;) {
    //        id1 = find(id1);
    //        id2 = find(id2);
    //
    //        if (id1 == id2)
    //            return id1;
    //
    //        uint32_t r1 = rank(id1), r2 = rank(id2);
    //
    //        if (r1 > r2 || (r1 == r2 && id1 < id2)) {
    //            std::swap(r1, r2);
    //            std::swap(id1, id2);
    //        }
    //
    //        uint64_t oldEntry = ((uint64_t) r1 << 32) | id1;
    //        uint64_t newEntry = ((uint64_t) r1 << 32) | id2;
    //
    //        if (!mData[id1].compare_exchange_strong(oldEntry, newEntry))
    //            continue;
    //
    //        if (r1 == r2) {
    //            oldEntry = ((uint64_t) r2 << 32) | id2;
    //            newEntry = ((uint64_t) (r2+1) << 32) | id2;
    //            /* Try to update the rank (may fail, that's ok) */
    //            mData[id2].compare_exchange_weak(oldEntry, newEntry);
    //        }
    //
    //        break;
    //    }
    //    return id2;
    //}

    @Override
    public void union(long id1, long id2) {
        while (true) {
            id1 = find(id1);
            id2 = find(id2);

            if (id1 == id2) {
                return;
            }

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

    // uint32_t size() const { return (uint32_t) mData.size(); }

    @Override
    public long size() {
        return data.size();
    }
}
