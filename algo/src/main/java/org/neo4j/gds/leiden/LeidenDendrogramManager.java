/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.leiden;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.concurrent.atomic.AtomicLong;

public class LeidenDendrogramManager {

    private final Graph rootGraph;
    private final long nodeCount;
    private final int concurrency;
    private final boolean trackIntermediateCommunities;
    private final HugeLongArray[] dendrograms;
    private int currentIndex;

    LeidenDendrogramManager(
        Graph rootGraph,
        int maxIterations,
        int concurrency,
        boolean trackIntermediateCommunities
    ) {
        this.rootGraph = rootGraph;
        this.nodeCount = rootGraph.nodeCount();
        this.concurrency = concurrency;
        if (trackIntermediateCommunities) {
            this.dendrograms = new HugeLongArray[maxIterations];
        } else {
            this.dendrograms = new HugeLongArray[1];
        }
        this.trackIntermediateCommunities = trackIntermediateCommunities;
    }

    public HugeLongArray[] getAllDendrograms() {
        return dendrograms;
    }

    public HugeLongArray getCurrent() {return dendrograms[currentIndex];}

    DendrogramResult setNextLevel(
        Graph workingGraph,
        HugeLongArray previousIterationDendrogram,
        HugeLongArray refinedCommunities,
        HugeLongArray localMoveCommunities,
        SeedCommunityManager seedCommunityManager,
        int iteration
    ) {
        assert workingGraph.nodeCount() == refinedCommunities.size() : "The sizes of the graph and communities should match";

        var dendrogram = HugeLongArray.newArray(rootGraph.nodeCount());

        prepareNextLevel(iteration);

        AtomicLong maxCommunityId = new AtomicLong(0L);
        ParallelUtil.parallelForEachNode(rootGraph, concurrency, (nodeId) -> {
            long prevId = previousIterationDendrogram == null
                ? nodeId
                : workingGraph.toMappedNodeId(previousIterationDendrogram.get(nodeId));

            long communityId = refinedCommunities.get(prevId);

            boolean updatedMaxCurrentId;
            do {
                var currentMaxId = maxCommunityId.get();
                if (communityId > currentMaxId) {
                    updatedMaxCurrentId = maxCommunityId.compareAndSet(currentMaxId, communityId);
                } else {
                    updatedMaxCurrentId = true;
                }
            } while (!updatedMaxCurrentId);

            dendrogram.set(nodeId, communityId);

            var reverseId = seedCommunityManager.mapToSeed(localMoveCommunities.get(communityId));
            set(nodeId, reverseId);
        });

        return ImmutableDendrogramResult.of(maxCommunityId.get(), dendrogram);
    }

    private void prepareNextLevel(int iteration) {
        currentIndex = trackIntermediateCommunities ? iteration : 0;
        if (currentIndex > 0 || iteration == 0) {
            dendrograms[currentIndex] = HugeLongArray.newArray(nodeCount);
        }
    }

    private void set(long nodeId, long communityId) {
        dendrograms[currentIndex].set(nodeId, communityId);
    }

    @ValueClass
    interface DendrogramResult {
        long maxCommunityId();

        HugeLongArray dendrogram();
    }
}
