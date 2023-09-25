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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.collections.ha.HugeByteArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class PlaceNodesRandomly {

    private final ApproxMaxKCutBaseConfig config;
    private final SplittableRandom random;
    private final Graph graph;
    private final List<Long> rangePartitionActualBatchSizes;
    private final ExecutorService executor;
    private final ProgressTracker progressTracker;

    PlaceNodesRandomly(
        ApproxMaxKCutBaseConfig config,
        SplittableRandom random,
        Graph graph,
        ExecutorService executor,
        ProgressTracker progressTracker
    ) {
        this.config = config;
        this.random = random;
        this.graph = graph;
        this.executor = executor;
        this.progressTracker = progressTracker;

        this.rangePartitionActualBatchSizes = PartitionUtils.rangePartitionActualBatchSizes(
            config.concurrency(),
            graph.nodeCount(),
            Optional.of(config.minBatchSize())
        );
    }

    void compute(HugeByteArray candidateSolution, AtomicLongArray currentCardinalities) {
        assert graph.nodeCount() >= config.k();

        var minCommunitiesPerPartition = minCommunitySizesToPartitions(rangePartitionActualBatchSizes);
        for (byte i = 0; i < config.k(); i++) {
            currentCardinalities.set(i, config.minCommunitySizes().get(i));
        }

        var partitionIndex = new AtomicInteger(0);
        var tasks = PartitionUtils.rangePartition(
            config.concurrency(),
            graph.nodeCount(),
            partition -> new AssignNodes(
                random.split(),
                candidateSolution,
                currentCardinalities,
                minCommunitiesPerPartition[partitionIndex.getAndIncrement()],
                partition
            ),
            Optional.of(config.minBatchSize())
        );
        progressTracker.beginSubTask();
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .executor(executor)
            .run();
        progressTracker.endSubTask();
    }

    // Assign the duty of assigning nodes to fill up minimum community size requirements among partitions in a
    // sufficiently random way.
    private long[][] minCommunitySizesToPartitions(List<Long> batchSizes) {
        // Balance granularity of communities' min sizes over partition such that it's sufficiently random while not
        // requiring too many iterations.
        double SIZE_TO_CHUNK_FACTOR = batchSizes.size() * 8.0;
        var chunkSizes = config
            .minCommunitySizes()
            .stream()
            .mapToLong(minSz -> (long) Math.ceil(minSz / SIZE_TO_CHUNK_FACTOR))
            .toArray();

        var currPartitionCounts = new long[batchSizes.size()];
        var remainingMinCommunitySizeCounts = new ArrayList<>(config.minCommunitySizes());

        var minCommunitiesPerPartition = new long[config.concurrency()][];
        Arrays.setAll(minCommunitiesPerPartition, i -> new long[config.k()]);

        var activePartitions = IntStream
            .range(0, batchSizes.size())
            .filter(partition -> batchSizes.get(partition) > 0)
            .boxed()
            .collect(Collectors.toList());
        var activeCommunities = IntStream
            .range(0, config.k())
            .filter(community -> config.minCommunitySizes().get(community) > 0)
            .boxed()
            .collect(Collectors.toList());

        while (!activeCommunities.isEmpty()) {
            int partitionIdx = random.nextInt(activePartitions.size());
            int communityIdx = random.nextInt(activeCommunities.size());
            int partition = activePartitions.get(partitionIdx);
            int community = activeCommunities.get(communityIdx);
            long increment = Math.min(
                Math.min(chunkSizes[community], batchSizes.get(partition) - currPartitionCounts[partition]),
                remainingMinCommunitySizeCounts.get(community)
            );

            minCommunitiesPerPartition[partition][community] += increment;
            currPartitionCounts[partition] += increment;
            if (currPartitionCounts[partition] == batchSizes.get(partition)) {
                activePartitions.remove(partitionIdx);
            }

            remainingMinCommunitySizeCounts.set(community, remainingMinCommunitySizeCounts.get(community) - increment);
            if (remainingMinCommunitySizeCounts.get(community) == 0) {
                activeCommunities.remove(communityIdx);
            }
        }

        return minCommunitiesPerPartition;
    }

    private final class AssignNodes implements Runnable {

        private final SplittableRandom random;
        private final HugeByteArray candidateSolution;
        private final AtomicLongArray cardinalities;
        private final long[] minNodesPerCommunity;
        private final Partition partition;
        private final byte k;

        AssignNodes(
            SplittableRandom random, HugeByteArray candidateSolution,
            AtomicLongArray cardinalities,
            long[] minNodesPerCommunity,
            Partition partition
        ) {
            this.random = random;
            this.candidateSolution = candidateSolution;
            this.cardinalities = cardinalities;
            this.minNodesPerCommunity = minNodesPerCommunity;
            this.partition = partition;
            this.k = config.k();
        }

        @Override
        public void run() {
            var nodes = shuffle(partition.startNode(), partition.nodeCount());

            // Fill in the nodes that this partition is required to provide to each community.
            long offset = 0;
            for (byte i = 0; i < k; i++) {
                for (long j = 0; j < minNodesPerCommunity[i]; j++) {
                    candidateSolution.set(nodes.get(offset++), i);
                }
            }

            // Assign the rest of the nodes of the partition to random communities.
            final var localCardinalities = new long[k];
            for (long i = offset; i < nodes.size(); i++) {
                byte randomCommunity = (byte) random.nextInt(0, k);
                localCardinalities[randomCommunity]++;
                candidateSolution.set(nodes.get(i), randomCommunity);
            }

            for (int i = 0; i < k; i++) {
                cardinalities.addAndGet(i, localCardinalities[i]);
            }

            progressTracker.logProgress(partition.nodeCount());
        }

        private HugeLongArray shuffle(long minInclusive, long length) {
            HugeLongArray elements = HugeLongArray.newArray(length);

            for (long i = 0; i < length; i++) {
                long nextToAdd = minInclusive + i;
                long j = random.nextLong(0, i + 1);
                if (j == i) {
                    elements.set(i, nextToAdd);
                } else {
                    elements.set(i, elements.get(j));
                    elements.set(j, nextToAdd);
                }
            }

            return elements;
        }
    }
}
