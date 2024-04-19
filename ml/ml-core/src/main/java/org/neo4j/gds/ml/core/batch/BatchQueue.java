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
package org.neo4j.gds.ml.core.batch;

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BatchQueue {

    public static final int DEFAULT_BATCH_SIZE = 100;

    final long totalSize;
    final int batchSize;
    long currentBatch;

    BatchQueue(long totalSize, int batchSize) {
        this.totalSize = totalSize;
        this.batchSize = batchSize;
        this.currentBatch = 0;
    }

    public static int computeBatchSize(long totalSize, int minBatchSize, int concurrency) {
        return Math.toIntExact(Math.min(
            Integer.MAX_VALUE,
            ParallelUtil.adjustedBatchSize(totalSize, concurrency, minBatchSize)
        ));
    }

    public static BatchQueue consecutive(long totalSize) {
        return consecutive(totalSize, DEFAULT_BATCH_SIZE);
    }

    public static BatchQueue consecutive(long totalSize, int minBatchSize, int concurrency) {
        return consecutive(totalSize, computeBatchSize(totalSize, minBatchSize, concurrency));
    }

    public static BatchQueue consecutive(long totalSize, int batchSize) {
        return new ConsecutiveBatchQueue(totalSize, batchSize);
    }

    public static BatchQueue fromArray(ReadOnlyHugeLongArray data) {
        return fromArray(data, DEFAULT_BATCH_SIZE);
    }

    public static BatchQueue fromArray(ReadOnlyHugeLongArray data, int batchSize) {
        return new ArraySourcedBatchQueue(data, batchSize);
    }



    abstract Optional<Batch> pop();

    public long totalSize() {
        return totalSize;
    }

    public void parallelConsume(Consumer<Batch> consumer, Concurrency concurrency, TerminationFlag terminationFlag) {
        parallelConsume(concurrency, ignore -> consumer, terminationFlag);
    }

    public void parallelConsume(Concurrency concurrency, List<? extends Consumer<Batch>> consumers, TerminationFlag terminationFlag) {
        assert consumers.size() == concurrency.value();

        var tasks = consumers.stream().map(ConsumerTask::new);
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .run();
    }

    public void parallelConsume(Concurrency concurrency, IntFunction<? extends Consumer<Batch>> consumerSupplier, TerminationFlag terminationFlag) {
        var consumers = IntStream
            .range(0, concurrency.value())
            .mapToObj(consumerSupplier)
            .collect(Collectors.toList());

        parallelConsume(concurrency, consumers, terminationFlag);
    }

    private class ConsumerTask implements Runnable {
        private final Consumer<Batch> batchConsumer;

        ConsumerTask(Consumer<Batch> batchConsumer) {this.batchConsumer = batchConsumer;}

        @Override
        public void run() {
            Optional<Batch> maybeBatch;
            while ((maybeBatch = pop()).isPresent()) {
                var batch = maybeBatch.get();
                batchConsumer.accept(batch);
            }
        }
    }
}
