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
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.LongToDoubleFunction;

public final class CentralityStatistics {

    public static DoubleHistogram histogram(
        long nodeCount,
        LongToDoubleFunction centralityFunction,
        ExecutorService executorService,
        int concurrency
    ) {
        DoubleHistogram histogram;

        if (concurrency == 1) {
            histogram = new DoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT);
            for (long id = 0; id < nodeCount; id++) {
                histogram.recordValue(centralityFunction.applyAsDouble(id));
            }
        } else {
            var tasks = PartitionUtils.rangePartition(
                concurrency,
                nodeCount,
                partition -> new RecordTask(partition, centralityFunction),
                Optional.empty()
            );

            ParallelUtil.run(tasks, executorService);

            histogram = new DoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT);
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

        RecordTask(Partition partition, LongToDoubleFunction centralityFunction) {
            this.partition = partition;
            this.centralityFunction = centralityFunction;
            this.histogram = new DoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT);
        }

        @Override
        public void run() {
            partition.consume(id -> {
                histogram.recordValue(centralityFunction.applyAsDouble(id));
            });
        }
    }
}
