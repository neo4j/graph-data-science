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
package org.neo4j.graphalgo.impl.similarity;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.queue.QueueBasedSpliterator;
import org.neo4j.graphalgo.results.SimilarityResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SimilarityStreamGenerator<T> {
    private final TerminationFlag terminationFlag;
    private final Supplier<RleDecoder> decoderFactory;
    private final SimilarityComputer<T> computer;
    private final int concurrency;

    public SimilarityStreamGenerator(TerminationFlag terminationFlag, ProcedureConfiguration configuration, Supplier<RleDecoder> decoderFactory, SimilarityComputer<T> computer) {
        this(terminationFlag, configuration.concurrency(), decoderFactory, computer);
    }

    public SimilarityStreamGenerator(TerminationFlag terminationFlag, int concurrency, Supplier<RleDecoder> decoderFactory, SimilarityComputer<T> computer) {
        this.terminationFlag = terminationFlag;
        this.concurrency = concurrency;
        this.decoderFactory = decoderFactory;
        this.computer = computer;
    }

    public Stream<SimilarityResult> stream(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, double cutoff, int topK) {
        int length = inputs.length;
        if (concurrency == 1) {
            if (topK != 0) {
                return similarityStreamTopK(inputs, sourceIndexIds, targetIndexIds, length, cutoff, topK, computer, decoderFactory);
            } else {
                return similarityStream(inputs, sourceIndexIds, targetIndexIds, length, cutoff, computer, decoderFactory);
            }
        } else {
            if (topK != 0) {
                return similarityParallelStreamTopK(inputs, sourceIndexIds, targetIndexIds, length, terminationFlag, concurrency, cutoff, topK, computer, decoderFactory);
            } else {
                return similarityParallelStream(inputs, sourceIndexIds, targetIndexIds, length, terminationFlag, concurrency, cutoff, computer, decoderFactory);
            }
        }
    }

    public Stream<SimilarityResult> stream(T[] inputs, double cutoff, int topK) {
        int length = inputs.length;
        if (concurrency == 1) {
            if (topK != 0) {
                return similarityStreamTopK(inputs, length, cutoff, topK, computer, decoderFactory);
            } else {
                return similarityStream(inputs, length, cutoff, computer, decoderFactory);
            }
        } else {
            if (topK != 0) {
                return similarityParallelStreamTopK(inputs, length, terminationFlag, concurrency, cutoff, topK, computer, decoderFactory);
            } else {
                return similarityParallelStream(inputs, length, terminationFlag, concurrency, cutoff, computer, decoderFactory);
            }
        }
    }

    // All Pairs

    private Stream<SimilarityResult> similarityStreamTopK(T[] inputs, int length, double cutoff, int topK, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        TopKConsumer<SimilarityResult>[] topKHolder = TopKConsumer.initializeTopKConsumers(length, topK);
        RleDecoder decoder = decoderFactory.get();

        SimilarityConsumer consumer = TopKConsumer.assignSimilarityPairs(topKHolder);
        for (int sourceId = 0; sourceId < length; sourceId++) {
            computeSimilarityForSourceIndex(sourceId, inputs, length, cutoff, consumer, computer, decoder);
        }
        return Arrays.stream(topKHolder).flatMap(TopKConsumer::stream);
    }

    private Stream<SimilarityResult> similarityStream(T[] inputs, int length, double cutoff, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        RleDecoder decoder = decoderFactory.get();
        return IntStream.range(0, length)
                .boxed().flatMap(sourceId -> IntStream.range(sourceId + 1, length)
                        .mapToObj(targetId -> computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff)).filter(Objects::nonNull));
    }

    private Stream<SimilarityResult> similarityParallelStream(T[] inputs, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustedBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + (length % batchSize > 0 ? 1 : 0);
        Collection<Runnable> tasks = new ArrayList<>(taskCount);

        ArrayBlockingQueue<SimilarityResult> queue = new ArrayBlockingQueue<>(queueSize);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            int taskOffset = taskId;
            tasks.add(() -> {
                RleDecoder decoder = decoderFactory.get();
                for (int offset = 0; offset < batchSize; offset++) {
                    int sourceId = taskOffset * multiplier + offset;
                    if (sourceId < length)
                        computeSimilarityForSourceIndex(sourceId, inputs, length, cutoff, (s, t, result) -> put(queue, result), computer, decoder);
                }
            });
        }

        new Thread(() -> {
            try {
                ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            } finally {
                put(queue, SimilarityResult.TOMB);
            }
        }).start();

        QueueBasedSpliterator<SimilarityResult> spliterator = new QueueBasedSpliterator<>(queue, SimilarityResult.TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }

    private  Stream<SimilarityResult> similarityParallelStreamTopK(T[] inputs, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, int topK, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        int batchSize = ParallelUtil.adjustedBatchSize(length, concurrency, 1);
        int taskCount = (length / batchSize) + (length % batchSize > 0 ? 1 : 0);
        Collection<TopKTask<T>> tasks = new ArrayList<>(taskCount);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            tasks.add(new TopKTask<>(batchSize, taskId, multiplier, length, inputs, cutoff, topK, computer, decoderFactory.get()));
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);

        TopKConsumer<SimilarityResult>[] topKConsumers = TopKConsumer.initializeTopKConsumers(length, topK);
        for (Runnable task : tasks) ((TopKTask) task).mergeInto(topKConsumers);
        return Arrays.stream(topKConsumers).flatMap(TopKConsumer::stream);
    }

    public static <T> void computeSimilarityForSourceIndex(int sourceId, T[] inputs, int length, double cutoff, SimilarityConsumer consumer, SimilarityComputer<T> computer, RleDecoder decoder) {
        for (int targetId = sourceId + 1; targetId < length; targetId++) {
            SimilarityResult similarity = computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff);
            if (similarity != null) {
                consumer.accept(sourceId, targetId, similarity);
            }
        }
    }

    // All Pairs

    private Stream<SimilarityResult> similarityStream(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, int length, double cutoff, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        RleDecoder decoder = decoderFactory.get();

        IntStream sourceRange = idRange(sourceIndexIds, length);
        Function<Integer, IntStream> targetRange = targetRange(targetIndexIds, length);

        return sourceRange.boxed().flatMap(sourceId -> targetRange.apply(sourceId)
                .mapToObj(targetId -> sourceId == targetId ? null : computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff))
                .filter(Objects::nonNull));
    }

    private IntStream idRange(int[] indexIds, int length) {
        return indexIds.length > 0 ? Arrays.stream(indexIds) : IntStream.range(0, length);
    }

    private Function<Integer, IntStream> targetRange(int[] targetIndexIds, int length) {
        return (sourceId) -> idRange(targetIndexIds, length);
    }

    private Stream<SimilarityResult> similarityStreamTopK(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, int length, double cutoff, int topK, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        TopKConsumer<SimilarityResult>[] topKHolder = TopKConsumer.initializeTopKConsumers(length, topK);
        RleDecoder decoder = decoderFactory.get();

        IntStream sourceRange = idRange(sourceIndexIds, length);
        Function<Integer, IntStream> targetRange = targetRange(targetIndexIds, length);

        SimilarityConsumer consumer = TopKConsumer.assignSimilarityPairs(topKHolder);
        sourceRange.forEach(sourceId -> computeSimilarityForSourceIndex(sourceId, inputs, cutoff, consumer, computer, decoder, targetRange));

        return Arrays.stream(topKHolder).flatMap(TopKConsumer::stream);
    }


    private void computeSimilarityForSourceIndex(int sourceId, T[] inputs, double cutoff, SimilarityConsumer consumer, SimilarityComputer<T> computer, RleDecoder decoder, Function<Integer, IntStream> targetRange) {
        targetRange.apply(sourceId).forEach(targetId -> {
            if(sourceId != targetId) {
                SimilarityResult similarity = computer.similarity(decoder, inputs[sourceId], inputs[targetId], cutoff);

                if (similarity != null) {
                    consumer.accept(sourceId, targetId, similarity);
                }
            }
        });
    }

    private  Stream<SimilarityResult> similarityParallelStream(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        Supplier<IntStream> sourceRange = () -> idRange(sourceIndexIds, length);
        Function<Integer, IntStream> targetRange = targetRange(targetIndexIds, length);

        int sourceIdsLength = sourceIndexIds.length > 0 ? sourceIndexIds.length : length;

        int timeout = 100;
        int queueSize = 1000;

        int batchSize = ParallelUtil.adjustedBatchSize(sourceIdsLength, concurrency, 1);
        int taskCount = (sourceIdsLength / batchSize) + (sourceIdsLength % batchSize > 0 ? 1 : 0);
        Collection<Runnable> tasks = new ArrayList<>(taskCount);

        ArrayBlockingQueue<SimilarityResult> queue = new ArrayBlockingQueue<>(queueSize);

        int multiplier = batchSize < length ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            int taskOffset = taskId;
            tasks.add(() -> {
                RleDecoder decoder = decoderFactory.get();
                sourceRange.get().skip(taskOffset * multiplier).limit(batchSize).forEach(sourceId ->
                        computeSimilarityForSourceIndex(sourceId, inputs, cutoff, (s, t, result) -> put(queue, result), computer, decoder, targetRange));

            });
        }

        new Thread(() -> {
            try {
                ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);
            } finally {
                put(queue, SimilarityResult.TOMB);
            }
        }).start();

        QueueBasedSpliterator<SimilarityResult> spliterator = new QueueBasedSpliterator<>(queue, SimilarityResult.TOMB, terminationFlag, timeout);
        return StreamSupport.stream(spliterator, false);
    }


    private  void put(BlockingQueue<SimilarityResult> queue, SimilarityResult items) {
        try {
            queue.put(items);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private Stream<SimilarityResult> similarityParallelStreamTopK(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, int length, TerminationFlag terminationFlag, int concurrency, double cutoff, int topK, SimilarityComputer<T> computer, Supplier<RleDecoder> decoderFactory) {
        Supplier<IntStream> sourceRange = () -> idRange(sourceIndexIds, length);
        Function<Integer, IntStream> targetRange = targetRange(targetIndexIds, length);

        int sourceIdsLength = sourceIndexIds.length > 0 ? sourceIndexIds.length : length;

        int batchSize = ParallelUtil.adjustedBatchSize(sourceIdsLength, concurrency, 1);
        int taskCount = (sourceIdsLength / batchSize) + (sourceIdsLength % batchSize > 0 ? 1 : 0);
        Collection<SourceTargetTopKTask<T>> tasks = new ArrayList<>(taskCount);

        int multiplier = batchSize < sourceIdsLength ? batchSize : 1;
        for (int taskId = 0; taskId < taskCount; taskId++) {
            tasks.add(new SourceTargetTopKTask<>(batchSize, taskId, multiplier, length, inputs, cutoff, topK, computer, decoderFactory.get(), sourceRange, targetRange));
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, Pools.DEFAULT);

        TopKConsumer<SimilarityResult>[] topKConsumers = TopKConsumer.initializeTopKConsumers(length, topK);
        for (Runnable task : tasks) ((SourceTargetTopKTask) task).mergeInto(topKConsumers);
        return Arrays.stream(topKConsumers).flatMap(TopKConsumer::stream);
    }
}
