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
package org.neo4j.gds.result;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.procedures.LongLongProcedure;
import org.HdrHistogram.Histogram;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.LazyBatchCollection;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

public final class CommunityStatistics {

    private static final long EMPTY_COMMUNITY = 0L;

    public static HugeSparseLongArray communitySizes(
        long nodeCount,
        LongUnaryOperator communityFunction,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        var componentSizeBuilder = HugeSparseLongArray.builder(EMPTY_COMMUNITY);

        if (concurrency.value() == 1) {
            // For one thread, we can just iterate through the node space
            // to avoid allocating thread-local buffers for each batch.
            for (long nodeId = 0L; nodeId < nodeCount; nodeId++) {
                componentSizeBuilder.addTo(communityFunction.applyAsLong(nodeId), 1L);
            }
        } else {
            // To avoid thread-contention on builder.addTo(),
            // each AddTask creates a thread-local buffer to cache
            // community sizes before they are written to the builder.
            // The batchSize is the size of that buffer and we limit
            // it in order to reduce memory overhead for the parallel
            // execution.
            var batchSize = ParallelUtil.adjustedBatchSize(
                nodeCount,
                concurrency,
                ParallelUtil.DEFAULT_BATCH_SIZE,
                // 10 is just a magic number that has been proven
                // to perform well in benchmarking.
                ParallelUtil.DEFAULT_BATCH_SIZE * 10
            );

            var tasks = LazyBatchCollection.of(
                nodeCount,
                batchSize,
                (start, length) -> new AddTask(componentSizeBuilder, communityFunction, start, length)
            );

            ParallelUtil.run(tasks, executorService);
        }

        return componentSizeBuilder.build();
    }

    public static long communityCount(
        long nodeCount,
        LongUnaryOperator communityFunction,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        var communitySizes = communitySizes(
            nodeCount,
            communityFunction,
            executorService,
            concurrency
        );
        return communityCount(communitySizes, executorService, concurrency);
    }

    public static long communityCount(
        HugeSparseLongArray communitySizes,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        var capacity = communitySizes.capacity();

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            capacity,
            partition -> new CountTask(communitySizes, partition),
            Optional.empty()
        );

        ParallelUtil.run(tasks, executorService);

        var communityCount = 0L;
        for (CountTask task : tasks) {
            communityCount += task.count();
        }

        return communityCount;
    }
    public static CommunityCountAndHistogram communityCountAndHistogram(
        long nodeCount,
        LongUnaryOperator communityFunction,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        var communitySizes = communitySizes(
            nodeCount,
            communityFunction,
            executorService,
            concurrency
        );
        return communityCountAndHistogram(communitySizes, HistogramProvider::new, executorService, concurrency);
    }

    public static CommunityCountAndHistogram communityCountAndHistogram(
        long nodeCount,
        LongUnaryOperator communityFunction,
        Supplier<HistogramProvider> histogramSupplier,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        var communitySizes = communitySizes(
            nodeCount,
            communityFunction,
            executorService,
            concurrency
        );
        return communityCountAndHistogram(communitySizes, histogramSupplier, executorService, concurrency);
    }
    public static CommunityStats communityStats(
        long nodeCount,
        LongUnaryOperator communityFunction,
        ExecutorService executorService,
        Concurrency concurrency,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        return communityStats(nodeCount,communityFunction,
            HistogramProvider::new,
            executorService,
            concurrency,
            statisticsComputationInstructions
        );
    }
    public static CommunityStats communityStats(
        long nodeCount,
        LongUnaryOperator communityFunction,
        Supplier<HistogramProvider> histogramSupplier,
        ExecutorService executorService,
        Concurrency concurrency,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        long componentCount = 0;
        Optional<Histogram> maybeHistogram = Optional.empty();
        var computeMilliseconds = new AtomicLong(0);
        try(var ignored = ProgressTimer.start(computeMilliseconds::set)) {
            if (statisticsComputationInstructions.computeCountAndDistribution()) {
                var communityStatistics = CommunityStatistics.communityCountAndHistogram(
                    nodeCount,
                    communityFunction,
                    histogramSupplier,
                    executorService,
                    concurrency
                );

                componentCount = communityStatistics.componentCount();
                maybeHistogram = Optional.of(communityStatistics.histogram());
            } else if (statisticsComputationInstructions.computeCountOnly()) {
                componentCount = CommunityStatistics.communityCount(
                    nodeCount,
                    communityFunction,
                    executorService,
                    concurrency
                );
            }
        } catch (Exception e){
            if (e.getMessage().contains("is out of bounds for histogram, current covered range")) {
                return ImmutableCommunityStats.of(0,Optional.empty(), computeMilliseconds.get(), false);
            } else {
                throw e;
            }


        }

        return ImmutableCommunityStats.of(componentCount, maybeHistogram, computeMilliseconds.get(), true);
    }

    public static Map<String, Object> communitySummary(Optional<Histogram> histogram, boolean success) {
        if (!success){
            return  HistogramUtils.failure();
        }
        return histogram
            .map(HistogramUtils::communitySummary)
            .orElseGet(Collections::emptyMap);
    }

    @ValueClass
    @SuppressWarnings("immutables:incompat")
    public interface CommunityStats {
        long componentCount();

        Optional<Histogram> histogram();

        long computeMilliseconds();

        boolean success();
    }
    public static CommunityCountAndHistogram communityCountAndHistogram(
        HugeSparseLongArray communitySizes,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        return communityCountAndHistogram( communitySizes, HistogramProvider::new,executorService,concurrency );
    }

    public static CommunityCountAndHistogram communityCountAndHistogram(
        HugeSparseLongArray communitySizes,
        Supplier<HistogramProvider> histogramSupplier,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        Histogram histogram;
        var communityCount = 0L;

        if (concurrency.value() == 1) {
           var histogramProvider = histogramSupplier.get();
            histogram =  histogramProvider.get();

            var capacity = communitySizes.capacity();

            for (long communityId = 0; communityId < capacity; communityId++) {
                long communitySize = communitySizes.get(communityId);
                if (communitySize != EMPTY_COMMUNITY) {
                    communityCount++;
                    histogram.recordValue(communitySize);
                }
            }
        } else {
            var capacity = communitySizes.capacity();

            var tasks = PartitionUtils.rangePartition(
                concurrency,
                capacity,
                partition -> new CountAndRecordTask(communitySizes, partition, histogramSupplier.get()),
                Optional.empty()
            );

            ParallelUtil.run(tasks, executorService);

            // highestTrackableValue must be >= 2 * lowestDiscernibleValue (1)
            var highestTrackableValue = 2L;
            for (CountAndRecordTask task : tasks) {
                communityCount += task.count;
                if (task.histogram.getMaxValue() > highestTrackableValue) {
                    highestTrackableValue = task.histogram.getMaxValue();
                }
            }
            var histogramProvider = histogramSupplier.get();
            histogramProvider.withHighestTrackedValue(highestTrackableValue);
            histogram =  histogramProvider.get();

            for (CountAndRecordTask task : tasks) {
                histogram.add(task.histogram);
            }
        }

        return ImmutableCommunityCountAndHistogram.builder()
            .componentCount(communityCount)
            .histogram(histogram)
            .build();
    }

    private CommunityStatistics() {}

    @ValueClass
    @SuppressWarnings("immutables:incompat")
    public interface CommunityCountAndHistogram {
        long componentCount();

        Histogram histogram();

    }

    private static class AddTask implements Runnable {

        private final HugeSparseLongArray.Builder builder;

        private final LongUnaryOperator communityFunction;

        private final long startId;
        private final long length;

        // Use local buffer to avoid contention on GrowingBuilder.add().
        // This is especially useful, if the input has a skewed
        // distribution, i.e. most nodes end up in the same community.
        private final LongLongMap buffer;

        AddTask(
            HugeSparseLongArray.Builder builder,
            LongUnaryOperator communityFunction,
            long startId,
            long length
        ) {
            this.builder = builder;
            this.communityFunction = communityFunction;
            this.startId = startId;
            this.length = length;
            // safe cast, since max batch size less than Integer.MAX_VALUE
            this.buffer = new LongLongHashMap((int) length);
        }

        @Override
        public void run() {
            var endId = startId + length;
            for (long id = startId; id < endId; id++) {
                buffer.addTo(communityFunction.applyAsLong(id), 1L);
            }
            buffer.forEach((LongLongProcedure) builder::addTo);
        }
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
            partition.consume(id -> {
                if (communitySizes.get(id) != EMPTY_COMMUNITY) {
                    count++;
                }
            });
        }

        long count() {
            return count;
        }
    }

    private static class CountAndRecordTask implements Runnable {

        private final HugeSparseLongArray communitySizes;

        private final Partition partition;

        private final Histogram histogram;

        private long count;

        CountAndRecordTask(HugeSparseLongArray communitySizes, Partition partition,HistogramProvider histogramProvider) {
            this.communitySizes = communitySizes;
            this.partition = partition;
            this.histogram = histogramProvider.get();
        }

        @Override
        public void run() {
            partition.consume(id -> {
                long communitySize = communitySizes.get(id);
                if (communitySize != EMPTY_COMMUNITY) {
                    count++;
                    histogram.recordValue(communitySize);
                }
            });
        }
    }

}
