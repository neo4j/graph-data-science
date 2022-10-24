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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

final class LocalMovePhase {

    private final Graph graph;
    private final HugeLongArray currentCommunities;
    //Idx   - nodeId
    //Value
    //       - Unweighted : degree of node
    //       - Weighted   : sum of the relationships weights of the node
    private final HugeDoubleArray nodeVolumes;

    // Idx   - communityId
    // Value
    //       - Unweighted : sum of the degrees of the nodes in the community.
    //       - Weighted   : sum of the relationship weights of the nodes in the community.
    // Note: the values also count relationships to nodes outside the community.
    private final HugeDoubleArray communityVolumes;
    private final double gamma;

    private final HugeLongArray encounteredCommunities;
    private final HugeDoubleArray encounteredCommunitiesWeights;
    private long encounteredCommunityCounter = 0;
    private final int concurrency;

    long swaps;

    private long communityCount;

    static LocalMovePhase create(
        Graph graph,
        HugeLongArray seedCommunities,
        HugeDoubleArray nodeVolumes,
        HugeDoubleArray communityVolumes,
        double gamma,
        long communityCount,
        int concurrency
    ) {

        var encounteredCommunities = HugeLongArray.newArray(graph.nodeCount());
        var encounteredCommunitiesWeights = HugeDoubleArray.newArray(graph.nodeCount());
        encounteredCommunitiesWeights.setAll(c -> -1L);

        return new LocalMovePhase(
            graph,
            communityCount,
            seedCommunities,
            nodeVolumes,
            communityVolumes,
            encounteredCommunities,
            encounteredCommunitiesWeights,
            gamma,
            concurrency
        );
    }

    private LocalMovePhase(
        Graph graph,
        long communityCount,
        HugeLongArray seedCommunities,
        HugeDoubleArray nodeVolumes,
        HugeDoubleArray communityVolumes,
        HugeLongArray encounteredCommunities,
        HugeDoubleArray encounteredCommunitiesWeights,
        double gamma,
        int concurrency
    ) {
        this.graph = graph;
        this.communityCount = communityCount;
        this.currentCommunities = seedCommunities;
        this.gamma = gamma;
        this.nodeVolumes = nodeVolumes;
        this.communityVolumes = communityVolumes;
        this.encounteredCommunities = encounteredCommunities;
        this.encounteredCommunitiesWeights = encounteredCommunitiesWeights;
        this.swaps = 0;
        this.concurrency = concurrency;

    }

    /**
     *
     * @return The new community count.
     */
    public long run() {
        HugeAtomicDoubleArray atomicCommunityVolumes = HugeAtomicDoubleArray.newArray(graph.nodeCount());
        graph.forEachNode(v -> {
            atomicCommunityVolumes.set(v, communityVolumes.get(v));
            return true;
        });
        HugeLongArray globalQueue = HugeLongArray.newArray(graph.nodeCount());
        AtomicLong globalQueueIndex = new AtomicLong();
        AtomicLong globalQueueSize = new AtomicLong(graph.nodeCount());

        HugeAtomicBitSet nodeInQueue = HugeAtomicBitSet.create(graph.nodeCount());
        nodeInQueue.set(0, graph.nodeCount());
        graph.forEachNode(v -> {
            globalQueue.set(v, v);
            return true;
        });
        var tasks = new ArrayList<LocalMoveTask>();
        for (int i = 0; i < concurrency; ++i) {
            tasks.add(new LocalMoveTask(
                graph.concurrentCopy(),
                currentCommunities,
                atomicCommunityVolumes,
                nodeVolumes,
                globalQueue,
                globalQueueIndex,
                globalQueueSize,
                nodeInQueue,
                gamma
            ));
        }

        while (globalQueueSize.get() > 0) {
            globalQueueIndex.set(0); //exhaust global queue
            RunWithConcurrency.builder().tasks(tasks).concurrency(concurrency).run();
            globalQueueSize.set(0); //fill global queue again
            RunWithConcurrency.builder().tasks(tasks).concurrency(concurrency).run();
        }
        for (var task : tasks) {
            swaps += task.swaps;
        }

        MutableDouble aliveCommunities = new MutableDouble(graph.nodeCount());
        graph.forEachNode(v -> {
            communityVolumes.set(v, atomicCommunityVolumes.get(v));
            if (Double.compare(communityVolumes.get(v), 0.0) == 0) {
                aliveCommunities.decrement();
            }
            return true;
        });
        return aliveCommunities.longValue();
    }
    
}
