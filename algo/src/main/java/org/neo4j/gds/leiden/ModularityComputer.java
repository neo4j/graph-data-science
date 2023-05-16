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
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

public final class ModularityComputer {

    static MemoryEstimation estimation() {
        return MemoryEstimations.builder(ModularityComputer.class)
            .perNode("relationships outside community", HugeAtomicDoubleArray::memoryEstimation)
            .perThread("relationship calculator", MemoryEstimations.builder(OutsideRelationshipCalculator.class).build())
            .build();
    }

    private ModularityComputer() {}

    static double compute(
        Graph workingGraph,
        HugeLongArray communities,
        HugeDoubleArray communityVolumes,
        double gamma,
        double coefficient,
        int concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        var relationshipsOutsideCommunity = HugeAtomicDoubleArray.of(workingGraph.nodeCount(), ParallelDoublePageCreator.passThrough(concurrency));
        // using degreePartitioning did not show an improvement -- assuming as tasks are too small
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            workingGraph.nodeCount(),
            partition -> new OutsideRelationshipCalculator(
                partition,
                workingGraph,
                relationshipsOutsideCommunity,
                communities,
                progressTracker
            ), Optional.empty()
        );
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();

        double modularity = ParallelUtil.parallelStream(
            LongStream.range(0, workingGraph.nodeCount()),
            concurrency,
            nodeStream ->
                nodeStream
                    .mapToDouble(communityId -> {
                        double outsideRelationships = relationshipsOutsideCommunity.get(communityId);
                        double totalRelationships = communityVolumes.get(communityId);
                        double insideRelationships = totalRelationships - outsideRelationships;
                        return insideRelationships - totalRelationships * totalRelationships * gamma;
                    })
                    .reduce(Double::sum)
                    .orElseThrow(() -> new RuntimeException("Error while computing modularity"))
        );
        //we do not have the self-loops from previous merges, so we settle from calculating the outside edges between relationships
        //from that and the total sum of weights in communityVolumes we can calculate all inside edges

        return modularity * coefficient;
    }

    static class OutsideRelationshipCalculator implements Runnable {
        private final Partition partition;
        private final Graph localGraph;
        private final HugeAtomicDoubleArray relationshipsOutsideCommunity;
        private final HugeLongArray communities;
        private final ProgressTracker progressTracker;

        OutsideRelationshipCalculator(
            Partition partition,
            Graph graph,
            HugeAtomicDoubleArray relationshipsOutsideCommunity,
            HugeLongArray communities,
            ProgressTracker progressTracker
        ) {
            this.partition = partition;
            this.localGraph = graph.concurrentCopy();
            this.relationshipsOutsideCommunity = relationshipsOutsideCommunity;
            this.communities = communities;
            this.progressTracker = progressTracker;
        }

        @Override
        public void run() {
            long startNode = partition.startNode();
            long endNode = startNode + partition.nodeCount();
            for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
                long communityId = communities.get(nodeId);
                localGraph.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                    long tCommunityId = communities.get(t);
                    if (tCommunityId != communityId) {
                        relationshipsOutsideCommunity.getAndAdd(communityId, w);
                    }
                    return true;
                });
                progressTracker.logProgress();
            }
        }
    }
}
