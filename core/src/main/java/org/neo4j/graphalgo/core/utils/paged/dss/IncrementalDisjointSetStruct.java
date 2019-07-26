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
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;

/**
 * Implements {@link SequentialDisjointSetStruct} with support for incremental computation based on a previously computed mapping
 * between node ids and set ids.
 * Note that this does not use <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure#by_rank">Union by Rank</a>
 * but prefers the minimum set id instead when merging two sets.
 */
@Deprecated
public final class IncrementalDisjointSetStruct extends SequentialDisjointSetStruct {

    private static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations.builder(
            IncrementalDisjointSetStruct.class)
            .perNode("parent", HugeLongArray::memoryEstimation)
            .add("internalToProvidedIds", HugeLongLongMap.memoryEstimation())
            .build();

    private final HugeLongArray parent;
    private final HugeLongLongMap internalToProvidedIds;
    private final HugeWeightMapping communityMapping;
    private final long size;
    private long maxCommunity;

    public static MemoryEstimation memoryEstimation() {
        return IncrementalDisjointSetStruct.MEMORY_ESTIMATION;
    }

    /**
     * Initialize the struct with the given size.
     *
     * @param size number of elements (maximum node id)
     */
    public IncrementalDisjointSetStruct(
            long size,
            HugeWeightMapping communityMapping,
            AllocationTracker tracker) {
        this.parent = HugeLongArray.newArray(size, tracker);
        this.internalToProvidedIds = new HugeLongLongMap(size, tracker);
        this.communityMapping = communityMapping;
        this.size = size;
        init(tracker);
    }

    @Override
    public HugeLongArray parent() {
        return parent;
    }

    /**
     * reset the container
     */
    private void init(AllocationTracker tracker) {
        this.maxCommunity = communityMapping.getMaxValue(-1);

        final HugeLongLongMap internalMapping = new HugeLongLongMap(size, tracker);

        this.parent.setAll(nodeId -> {
            long parentValue = -1;
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
                internalToProvidedIds.addTo(nodeId, ++maxCommunity);
            }
            return parentValue;
        });
    }

    /**
     * element (node) count
     *
     * @return the element count
     */
    @Override
    public long size() {
        return size;
    }

    /**
     * find setId of element p without balancing optimization.
     *
     * @param nodeId the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    @Override
    public long find(final long nodeId) {
        long p = nodeId;
        long np;
        while ((np = parent.get(p)) != -1L) {
            p = np;
        }
        return p;
    }

    @Override
    public long setIdOf(final long nodeId) {
        long setId = find(nodeId);
        return setIdOfRoot(setId);
    }

    @Override
    long setIdOfRoot(final long rootId) {
        long setId = internalToProvidedIds.getOrDefault(rootId, -1);
        assert(setId != -1);
        return setId;
    }
}
