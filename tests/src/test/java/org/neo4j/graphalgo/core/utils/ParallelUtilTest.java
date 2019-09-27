/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import org.junit.jupiter.api.Test;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.core.huge.loader.HugeParallelGraphImporter;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.Status;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphalgo.core.utils.ParallelUtil.parallelStream;

final class ParallelUtilTest {

    @Test
    void shouldParallelizeStreams() {
        long firstNum = 1;
        long lastNum = 1_000_000;

        List<Long> list = LongStream.rangeClosed(firstNum, lastNum).boxed().collect(Collectors.toList());

        ForkJoinPool pool = Pools.FJ_POOL;
        ForkJoinPool commonPool = ForkJoinPool.commonPool();
        Stream<Long> stream = list.stream();

        long actualTotal = parallelStream(stream, (s) -> {
            assertTrue(s.isParallel());
            Thread thread = Thread.currentThread();
            assertTrue(thread instanceof ForkJoinWorkerThread);
            ForkJoinPool threadPool = ((ForkJoinWorkerThread) thread).getPool();
            assertSame(threadPool, pool);
            assertNotSame(threadPool, commonPool);

            return s.reduce(0L, Long::sum);
        });

        assertEquals((lastNum + firstNum) * lastNum / 2, actualTotal);
    }

    @Test
    void shouldTakeBaseStreams() {
        double[] data = {1.0, 2.5, 3.14};

        double sum = parallelStream(Arrays.stream(data), DoubleStream::sum);

        assertEquals(1.0 + 2.5 + 3.14, sum);
    }

    @Test
    void threadSizeShouldDivideByBatchsize() {
        assertEquals(42, ParallelUtil.threadCount(1337, 1337 * 42));
    }

    @Test
    void threadSizeShouldReturnEnoughThreadsForIncompleteBatches() {
        int batchSize = 1337;
        int threads = 42;
        int elements = batchSize * threads + 21;
        assertEquals(threads + 1, ParallelUtil.threadCount(batchSize, elements));
    }

    @Test
    void threadSizeShouldReturn1ForNonCompleteBatches() {
        int batchSize = 1337;
        assertEquals(
                1,
                ParallelUtil.threadCount(batchSize, batchSize - 42));
    }

    @Test
    void threadSizeShouldReturn1ForZeroElements() {
        int batchSize = 1337;
        assertEquals(1, ParallelUtil.threadCount(batchSize, 0));
    }

    @Test
    void threadSizeShouldFailForZeroBatchsize() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ParallelUtil.threadCount(0, 42));
        assertEquals("Invalid batch size: 0", exception.getMessage());
    }

    @Test
    void threadSizeShouldFailForNegativeBatchsize() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ParallelUtil.threadCount(-1337, 42));
        assertEquals("Invalid batch size: -1337", exception.getMessage());
    }

    @Test
    void shouldRunBatchesSequentialIfNoExecutorIsGiven() {
        PrimitiveLongIterable[] ints = {longs(0, 10), longs(10, 14)};
        BatchNodeIterable batches = mock(BatchNodeIterable.class);
        when(batches.batchIterables(anyInt())).thenReturn(asList(ints));
        long currentThreadId = Thread.currentThread().getId();
        Runnable task = () -> {
            assertEquals(Thread.currentThread().getId(), currentThreadId);
        };
        HugeParallelGraphImporter importer = mock(HugeParallelGraphImporter.class);
        when(importer.newImporter(anyLong(), any())).thenReturn(task);

        ParallelUtil.readParallel(
                100,
                10,
                batches,
                null,
                importer
        );

        verify(importer, times(1)).newImporter(0, ints[0]);
        verify(importer, times(1)).newImporter(10, ints[1]);
    }

    @Test
    void batchingShouldCatenatePartitions() {
        int minBatchSize = 21;
        int maxConcurrency = 8;
        int nodeCount = 1337;

        String params = String.format(
                " [bs=%d,c=%d,n=%d]",
                minBatchSize,
                maxConcurrency,
                nodeCount);

        int batchSize = ParallelUtil.adjustedBatchSize(
                nodeCount,
                maxConcurrency,
                minBatchSize);

        assertTrue(
                batchSize >= minBatchSize,
                "batchSize smaller than minSize" + params);
        assertTrue(
                (int) Math.ceil(nodeCount / (double) batchSize) <= maxConcurrency,
                "batchSize too small to satisfy desired concurrency" + params);
    }

    @Test
    void shouldRunAtMostConcurrencyTasks() {
        int tasks = 6;
        int concurrency = 2;
        int threads = 4;
        withPool(threads, pool -> {
            final Tasks ts = new Tasks(tasks, 0);
            ParallelUtil.runWithConcurrency(concurrency, ts, pool);
            assertTrue(ts.maxRunning() <= concurrency);
            assertEquals(tasks, ts.started());
            assertEquals(tasks, ts.requested());
        });
    }

    @Test
    void shouldRunSequentially() {
        withPool(4, pool -> {
            ExecutorService deadPool = Executors.newFixedThreadPool(4);
            deadPool.shutdown();
            List<Consumer<Tasks>> runs = asList(
                    // null pool
                    t -> ParallelUtil.runWithConcurrency(8, t, null),
                    // terminated pool
                    t -> ParallelUtil.runWithConcurrency(8, t, deadPool),
                    // single task
                    t -> ParallelUtil.runWithConcurrency(8, t.sized(1), pool),
                    // concurrency = 1
                    t -> ParallelUtil.runWithConcurrency(1, t, pool),
                    // concurrency = 0
                    t -> ParallelUtil.runWithConcurrency(0, t, pool)
            );

            for (Consumer<Tasks> run : runs) {
                Tasks tasks = new Tasks(5, 10);
                tasks.run(run);
                assertEquals(tasks.size(), tasks.started());
                assertEquals(1, tasks.maxRunning());
                assertEquals(tasks.size(), tasks.requested());
            }
        });
    }

    @Test
    void shouldSubmitAtMostConcurrencyTasksRunSequentially() {
        withPool(4, pool -> {
            Tasks tasks = new Tasks(4, 10);
            tasks.run(t -> ParallelUtil.runWithConcurrency(2, t, pool));
            assertEquals(4, tasks.started());
            assertTrue(tasks.maxRunning() <= 2);
            assertEquals(4, tasks.requested());
        });
    }

    @Test
    void shouldBailOnFullThreadpoolAfterTrying() {
        ThreadPoolExecutor pool = mock(ThreadPoolExecutor.class);
        when(pool.getActiveCount()).thenReturn(Integer.MAX_VALUE);
        Tasks tasks = new Tasks(5, 10);
        try {
            tasks.run(t -> ParallelUtil.runWithConcurrency(4, t, 100, pool));
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Attempted to submit tasks"));
        }
        assertEquals(0, tasks.started());
        assertEquals(0, tasks.maxRunning());
        assertEquals(1, tasks.requested());
    }

    @Test
    void shouldBailOnThreadInterrupt() {
        withPool(4, pool -> {
            Tasks tasks = new Tasks(6, 10);
            final Thread thread = new Thread(() -> tasks.run(t ->
                    ParallelUtil.runWithConcurrency(2, t, pool)));
            thread.setUncaughtExceptionHandler((t, e) -> {
                assertEquals("Unexpected Exception", e.getMessage());
                assertEquals(
                        InterruptedException.class,
                        e.getCause().getClass());
            });

            thread.start();
            thread.interrupt();
            thread.join();

            assertTrue(tasks.started() <= 2);
            assertTrue(tasks.maxRunning() <= 2);
            assertTrue(tasks.requested() <= 2);
        });
    }

    @Test
    void shouldBailOnTermination() {
        TransactionTerminatedException exception = assertThrows(
                TransactionTerminatedException.class,
                () -> {
                    withPool(4, pool -> {
                        Tasks tasks = new Tasks(6, 100);
                        AtomicReference<RuntimeException> thrownException = new AtomicReference<>();
                        AtomicBoolean running = new AtomicBoolean(true);
                        TerminationFlag isRunning = running::get;
                        final Thread thread = new Thread(() -> tasks.run(t ->
                                ParallelUtil.runWithConcurrency(2, t, isRunning, pool)));

                        thread.setUncaughtExceptionHandler((t, e) -> thrownException.set(Exceptions.launderedException(e)));
                        thread.start();
                        running.set(false);
                        thread.join();

                        if (thrownException.get() != null) {
                            throw thrownException.get();
                        }

                        assertTrue(tasks.started() <= 2);
                        assertTrue(tasks.maxRunning() <= 2);
                        assertTrue(tasks.requested() <= 2);
                    });
                });
        assertEquals(Status.Transaction.Terminated, exception.status());
    }

    @Test
    void shouldWaitThenThrowOnFullThreadpool() {
        ThreadPoolExecutor pool = mock(ThreadPoolExecutor.class);
        when(pool.getActiveCount()).thenReturn(Integer.MAX_VALUE);
        Tasks tasks = new Tasks(5, 10);
        try {
            tasks.run(t -> ParallelUtil.runWithConcurrency(
                    4,
                    t,
                    10,
                    5,
                    TimeUnit.MILLISECONDS,
                    pool));
        } catch (IllegalThreadStateException e) {
            assertThat(e.getMessage(), containsString("Attempted to submit tasks"));
        }
        assertEquals(0, tasks.started());
        assertEquals(0, tasks.maxRunning());
        assertEquals(1, tasks.requested());
        verify(pool, times(11)).getActiveCount();
    }

    private static void withPool(
            int nThreads,
            ThrowingConsumer<ExecutorService, ? extends Throwable> block) {
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        try {
            block.accept(pool);
        } catch (Throwable throwable) {
            throw Exceptions.launderedException(throwable);
        } finally {
            List<Runnable> unscheduled = pool.shutdownNow();
            pool.shutdown();
            try {
                pool.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                // ok
            }
            assertTrue(unscheduled.isEmpty());
            assertTrue(pool.isTerminated());
        }
    }

    private PrimitiveLongIterable longs(int from, int size) {
        return () -> PrimitiveLongCollections.range(from, from + size - 1);
    }
}
