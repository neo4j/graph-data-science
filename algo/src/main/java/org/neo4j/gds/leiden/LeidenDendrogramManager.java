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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

public class LeidenDendrogramManager {

    static MemoryEstimation memoryEstimation(int numberOfTrackedIterations) {
        return MemoryEstimations.builder(LeidenDendrogramManager.class)
            .perNode("dendograms", HugeLongArray::memoryEstimation)
            .build()
            .times(numberOfTrackedIterations);
    }

    private final Graph rootGraph;
    private final long nodeCount;
    private final int concurrency;
    private final TerminationFlag terminationFlag;
    private final boolean trackIntermediateCommunities;
    private final HugeLongArray[] dendrograms;
    private int currentIndex;

    LeidenDendrogramManager(
        Graph rootGraph,
        int maxIterations,
        int concurrency,
        boolean trackIntermediateCommunities,
        TerminationFlag terminationFlag
    ) {
        this.rootGraph = rootGraph;
        this.nodeCount = rootGraph.nodeCount();
        this.concurrency = concurrency;
        this.terminationFlag = terminationFlag;
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


    //find which node corresponds to the community this node was merged to before
    private long translateNode(
        Graph workingGraph,
        HugeLongArray previousAlgorithmDendrogram,
        long nodeId,
        int iteration
    ) {
        return (iteration == 0)
            ? nodeId
            : workingGraph.toMappedNodeId(previousAlgorithmDendrogram.get(nodeId));
    }

    void updateOutputDendrogram(
        Graph workingGraph,
        HugeLongArray previousAlgorithmDendrogram,
        HugeLongArray communitiesToOuput,
        SeedCommunityManager seedCommunityManager,
        int iteration
    ) {
        assert workingGraph.nodeCount() == communitiesToOuput.size() : "The sizes of the graph and communities should match";

        prepareNextLevel(iteration);

        //This is final output, we need to take seeds into account
        //and translate the community to output format; for this we need to take the initial seeding values into acount
        ParallelUtil.parallelForEachNode(rootGraph.nodeCount(), concurrency, terminationFlag, nodeId -> {
            long prevId = translateNode(workingGraph, previousAlgorithmDendrogram, nodeId, iteration);
            long communityId = communitiesToOuput.get(prevId);
            var reverseId = seedCommunityManager.mapToSeed(communityId);
            setToOutputDendrogram(
                nodeId,
                reverseId
            ); //This is final output, we need to take seeds into account
            //and translate the community to output format; for this we need to take the initial seeding values into acount
        });


    }

    void updateAlgorithmDendrogram(
        Graph workingGraph,
        HugeLongArray algorithmDendrogram,
        HugeLongArray communitiesToWrite,
        int iteration
    ) {

        HugeLongArray finalAlgorithmDendrogram = algorithmDendrogram;
        ParallelUtil.parallelForEachNode(rootGraph.nodeCount(), concurrency, TerminationFlag.RUNNING_TRUE, (nodeId) -> {
            long prevId = translateNode(workingGraph, finalAlgorithmDendrogram, nodeId, iteration);
            long communityId = communitiesToWrite.get(prevId);
            finalAlgorithmDendrogram.set(nodeId, communityId);
        });
        //recall: this array marks the community of node
        //but disregards the numbering implied by any seeds (works on the algorithm level).
    }

    private void prepareNextLevel(int iteration) {
        currentIndex = trackIntermediateCommunities ? iteration : 0;
        if (currentIndex > 0 || iteration == 0) {
            dendrograms[currentIndex] = HugeLongArray.newArray(nodeCount);
        }
    }

    private void setToOutputDendrogram(long nodeId, long communityId) {
        dendrograms[currentIndex].set(nodeId, communityId);
    }


}
