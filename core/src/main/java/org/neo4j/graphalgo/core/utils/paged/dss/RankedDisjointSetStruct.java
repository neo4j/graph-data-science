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


import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

/**
 * Implementation of {@link DisjointSetStruct} uses Union by Rank and Path compression.
 */
public final class RankedDisjointSetStruct extends DisjointSetStruct {

    public static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations.builder(RankedDisjointSetStruct.class)
            .perNode("parent", HugeLongArray::memoryEstimation)
            .perNode("depth", HugeLongArray::memoryEstimation)
            .build();

    private final HugeLongArray parent;
    private final long size;

    public static MemoryEstimation memoryEstimation() {
        return RankedDisjointSetStruct.MEMORY_ESTIMATION;
    }

    /**
     * Initialize the struct with the given size.
     *
     * @param size number of elements (maximum node id)
     */
    public RankedDisjointSetStruct(long size, UnionStrategy unionStrategy, AllocationTracker tracker) {
        super(unionStrategy);
        parent = HugeLongArray.newArray(size, tracker);
        this.size = size;
        parent.fill(-1L);
    }

    @Override
    public HugeLongArray parent() {
        return parent;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long find(long p) {
        return findWithPathCompression(p);
    }

    @Override
    long findNoOpt(final long nodeId) {
        long p = nodeId;
        long np;
        while ((np = parent.get(p)) != -1L) {
            p = np;
        }
        return p;
    }

    @Override
    public long setIdOf(final long nodeId) {
        return findNoOpt(nodeId);
    }

    @Override
    long setIdOfRoot(final long rootId) {
        return rootId;
    }
}
