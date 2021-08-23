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

import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BatchQueue {
    public static final int DEFAULT_BATCH_SIZE = 100;
    protected final long nodeCount;
    protected final int batchSize;
    long currentBatch;

    public BatchQueue(long nodeCount) {
        this(nodeCount, DEFAULT_BATCH_SIZE);
    }

    public BatchQueue(long nodeCount, int batchSize) {
        this.nodeCount = nodeCount;
        this.batchSize = batchSize;
        this.currentBatch = 0;
    }

    public BatchQueue(long nodeCount, int minBatchSize, int concurrency) {
        this(nodeCount,
            Math.toIntExact(Math.min(
                Integer.MAX_VALUE,
                ParallelUtil.adjustedBatchSize(nodeCount, concurrency, minBatchSize)
            ))
        );
    }

    synchronized Optional<Batch> pop() {
        if (currentBatch * batchSize >= nodeCount) {
            return Optional.empty();
        }
        var batch = new LazyBatch(currentBatch * batchSize, batchSize, nodeCount);
        currentBatch += 1;
        return Optional.of(batch);
    }

    public void parallelConsume(Consumer<Batch> consumer, int concurrency, TerminationFlag terminationFlag) {
        parallelConsume(concurrency, ignore -> consumer, terminationFlag);
    }

    public void parallelConsume(int concurrency, List<? extends Consumer<Batch>> consumers, TerminationFlag terminationFlag) {
        assert consumers.size() == concurrency;

        var tasks = consumers.stream().map(ConsumerTask::new).collect(Collectors.toList());
        ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
    }

    public void parallelConsume(int concurrency, IntFunction<? extends Consumer<Batch>> consumerSupplier, TerminationFlag terminationFlag) {
        var consumers = IntStream
            .range(0, concurrency)
            .mapToObj(consumerSupplier::apply)
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
