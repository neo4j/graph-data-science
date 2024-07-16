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

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.core.ProcedureConstants;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongToDoubleFunction;
import java.util.function.Supplier;

public final class CentralityStatistics {

    static DoubleHistogram histogram(
        long nodeCount,
        LongToDoubleFunction centralityFunction,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        return histogram(
            nodeCount,
            () -> new DoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT),
            centralityFunction,
            executorService,
            concurrency
        );
    }

    static DoubleHistogram histogram(
        long nodeCount,
        Supplier<DoubleHistogram> histogramSupplier,
        LongToDoubleFunction centralityFunction,
        ExecutorService executorService,
        Concurrency concurrency
    ) {
        var histogram = histogramSupplier.get();
        if (concurrency.value() == 1) {
            for (long id = 0; id < nodeCount; id++) {
                histogram.recordValue(centralityFunction.applyAsDouble(id));
            }
        } else {
            var tasks = PartitionUtils.rangePartition(
                concurrency,
                nodeCount,
                partition -> new RecordTask(partition, centralityFunction, histogramSupplier),
                Optional.empty()
            );

            ParallelUtil.run(tasks, executorService);

            for (var task : tasks) {
                histogram.add(task.histogram);
            }
        }
        return histogram;
    }

    private CentralityStatistics() {}

    private static class RecordTask implements Runnable {

        private final DoubleHistogram histogram;
        private final Partition partition;
        private final LongToDoubleFunction centralityFunction;

        RecordTask(
            Partition partition,
            LongToDoubleFunction centralityFunction,
            Supplier<DoubleHistogram> histogramSupplier
        ) {
            this.partition = partition;
            this.centralityFunction = centralityFunction;
            this.histogram = histogramSupplier.get();
        }

        @Override
        public void run() {
            partition.consume(id -> {
                histogram.recordValue(centralityFunction.applyAsDouble(id));
            });
        }
    }

    public static CentralityStats centralityStatistics(
        long nodeCount,
        LongToDoubleFunction centralityProvider,
        ExecutorService executorService,
        Concurrency concurrency,
        boolean shouldCompute
    ) {
        return computeCentralityStatistics(
            nodeCount,
            centralityProvider,
            executorService,
            concurrency,
            () -> new DoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT),
            shouldCompute
        );
    }

    public static CentralityStats computeCentralityStatistics(
        long nodeCount,
        LongToDoubleFunction centralityProvider,
        ExecutorService executorService,
        Concurrency concurrency,
        Supplier<DoubleHistogram> histogramSupplier,
        boolean shouldCompute
    ) {
        Optional<DoubleHistogram> maybeHistogram = Optional.empty();
        var computeMilliseconds = new AtomicLong(0);

        try (var ignored = ProgressTimer.start(computeMilliseconds::set)) {
            if (shouldCompute) {
                var histogram = histogram(
                    nodeCount,
                    histogramSupplier,
                    centralityProvider,
                    executorService,
                    concurrency
                );
                maybeHistogram = Optional.of(histogram);
            }
        } catch (Exception e) {
            return new CentralityStats(Optional.empty(), computeMilliseconds.get(), false);
        }

        return new CentralityStats(maybeHistogram, computeMilliseconds.get(), true);
    }

    public static Map<String, Object> centralitySummary(Optional<DoubleHistogram> histogram, boolean success) {
        if (!success) {
            return HistogramUtils.failure();
        }
        return histogram
            .map(HistogramUtils::centralitySummary)
            .orElseGet(Collections::emptyMap);
    }

    public record CentralityStats(Optional<DoubleHistogram> histogram, long computeMilliseconds, boolean success) {
    }


}
