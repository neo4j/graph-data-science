/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.statistics;

import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

public final class CommunityStatistics {

    private static final long EMPTY_COMMUNITY = 0L;

    public static HugeSparseLongArray communitySizes(long nodeCount, LongUnaryOperator communityFunction, AllocationTracker tracker) {
        var componentSizeBuilder = HugeSparseLongArray.GrowingBuilder.create(EMPTY_COMMUNITY, tracker);

        for (long nodeId = 0L; nodeId < nodeCount; nodeId++) {
            componentSizeBuilder.addTo(communityFunction.applyAsLong(nodeId), 1L);
        }

        return componentSizeBuilder.build();
    }

    public static long communityCount(long nodeCount, LongUnaryOperator communityFunction, AllocationTracker tracker) {
        return communityCount(communitySizes(nodeCount, communityFunction, tracker), Pools.DEFAULT, 1);
    }

    public static long communityCount(HugeSparseLongArray communitySizes, ExecutorService executorService, int concurrency) {
        var capacity = communitySizes.getCapacity();

        var tasks = PartitionUtils
            .rangePartition(concurrency, capacity)
            .stream()
            .map(partition -> new CountTask(communitySizes, partition))
            .collect(Collectors.toList());

        ParallelUtil.run(tasks, executorService);

        var communityCount = 0L;
        for (CountTask task : tasks) {
            communityCount += task.count();
        }

        return communityCount;
    }

    public static CommunityCountAndHistogram communityCountAndHistogram(long nodeCount, LongUnaryOperator communityFunction, AllocationTracker tracker) {
        return communityCountAndHistogram(communitySizes(nodeCount, communityFunction, tracker));
    }

    public static CommunityCountAndHistogram communityCountAndHistogram(HugeSparseLongArray communitySizes) {
        var histogram = new Histogram(5);
        var communityCount = 0L;
        var capacity = communitySizes.getCapacity();

        for (long communityId = 0; communityId < capacity; communityId++) {
            long communitySize = communitySizes.get(communityId);
            if (communitySize != EMPTY_COMMUNITY) {
                communityCount++;
                histogram.recordValue(communitySize);
            }
        }

        return ImmutableCommunityCountAndHistogram.builder()
            .componentCount(communityCount)
            .histogram(histogram)
            .build();
    }

    private CommunityStatistics() {}

    @ValueClass
    public interface CommunityCountAndHistogram {
        long componentCount();

        Histogram histogram();
    }

    private static class CountTask implements Runnable {

        private final HugeSparseLongArray communitySizes;
        private final Partition partition;

        private long count;

        CountTask(HugeSparseLongArray communitySizes, Partition partition) {
            this.communitySizes = communitySizes;
            this.partition = partition;
        }

        @Override
        public void run() {
            long startId = partition.startNode();
            long endId = partition.startNode() + partition.nodeCount();

            for (long id = startId; id < endId; id++) {
                if (communitySizes.get(id) != EMPTY_COMMUNITY) {
                    count++;
                }
            }
        }

        public long count() {
            return count;
        }
    }

}
