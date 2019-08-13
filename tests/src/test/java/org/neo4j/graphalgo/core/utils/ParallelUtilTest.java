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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.helpers.Exceptions;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public final class ParallelUtilTest extends RandomizedTest {

    @Test
    public void shouldParallelizeStreams() throws ExecutionException, InterruptedException {
        long firstNum = 1;
        long lastNum = 1_000_000;

        List<Long> list = LongStream.rangeClosed(firstNum, lastNum).boxed().collect(Collectors.toList());

        ForkJoinPool pool = Pools.FJ_POOL;
        ForkJoinPool commonPool = ForkJoinPool.commonPool();
        long actualTotal = ParallelUtil.parallelStream(list, (s) -> {
            assertThat(s.isParallel(), equalTo(true));
            return s.reduce(0L, Long::sum);
        }, pool);

        assertThat(commonPool.getStealCount(), equalTo(0L));
        assertEquals((lastNum + firstNum) * lastNum / 2, actualTotal);
    }

    @Test
    public void threadSizeShouldDivideByBatchsize() throws Exception {
        int batchSize = between(10, 10_000);
        int threads = between(10, 10_000);
        assertEquals(
                threads,
                ParallelUtil.threadCount(batchSize, batchSize * threads));
    }

    @Test
    public void threadSizeShouldReturnEnoughThreadsForIncompleteBatches() throws
            Exception {
        int batchSize = between(10, 10_000);
        int threads = between(10, 10_000);
        int elements = batchSize * threads + between(1, batchSize - 1);
        assertEquals(threads + 1, ParallelUtil.threadCount(batchSize, elements));
    }

    @Test
    public void threadSizeShouldReturn1ForNonCompleteBatches() throws
            Exception {
        int batchSize = between(2, 1_000_000);
        assertEquals(
                1,
                ParallelUtil.threadCount(
                        batchSize,
                        batchSize - randomInt(batchSize)));
    }

    @Test
    public void threadSizeShouldReturn1ForZeroElements() throws Exception {
        int batchSize = between(2, 1_000_000);
        assertEquals(1, ParallelUtil.threadCount(batchSize, 0));
    }

    @Test
    public void threadSizeShouldFailForZeroBatchsize() throws Exception {
        try {
            ParallelUtil.threadCount(0, randomInt());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: 0", e.getMessage());
        }
    }

    @Test
    public void threadSizeShouldFailForNegativeBatchsize() throws Exception {
        int batchSize = between(Integer.MIN_VALUE, -1);
        try {
            ParallelUtil.threadCount(batchSize, randomInt());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: " + batchSize, e.getMessage());
        }
    }

    @Test
    public void shouldRunBatchesSequentialIfNoExecutorIsGiven() {
        PrimitiveIntIterable[] ints = {ints(0, 10), ints(10, 14)};
        IntIdMap batches = mock(IntIdMap.class);
        when(batches.batchIterables(anyInt())).thenReturn(asList(ints));
        Runnable task = () -> {
        };
        ParallelGraphImporter importer = mock(ParallelGraphImporter.class);
        when(importer.newImporter(anyInt(), any())).thenReturn(task);

        final Collection tasks = ParallelUtil.readParallel(
                100,
                10,
                batches,
                importer,
                null);

        verify(importer, times(1)).newImporter(0, ints[0]);
        verify(importer, times(1)).newImporter(10, ints[1]);
        assertEquals(2, tasks.size());
        for (Object t : tasks) {
            assertSame(task, t);
        }
    }

    @Test
    public void batchingShouldCatenatePartitions() throws Exception {
        int minBatchSize = randomIntBetween(5, 25);
        int maxConcurrency = randomIntBetween(2, 10);
        int nodeCount = randomIntBetween(500, 1500);

        String params = String.format(
                " [bs=%d,c=%d,n=%d]",
                minBatchSize,
                maxConcurrency,
                nodeCount);

        int batchSize = ParallelUtil.adjustBatchSize(
                nodeCount,
                maxConcurrency,
                minBatchSize);

        assertTrue(
                "batchSize smaller than minSize" + params,
                batchSize >= minBatchSize);
        assertTrue(
                "batchSize too small to satisfy desired concurrency" + params,
                (int) Math.ceil(nodeCount / (double) batchSize) <= maxConcurrency);
    }

    @Test
    public void shouldRunAtMostConcurrencyTasks() {
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
    public void shouldRunSequentially() throws Exception {
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
    public void shouldSubmitAtMostConcurrencyTasksRunSequentially() throws Exception {
        withPool(4, pool -> {
            Tasks tasks = new Tasks(4, 10);
            tasks.run(t -> ParallelUtil.runWithConcurrency(2, t, pool));
            assertEquals(4, tasks.started());
            assertTrue(tasks.maxRunning() <= 2);
            assertEquals(4, tasks.requested());
        });
    }

    @Test
    public void shouldBailOnFullThreadpoolAfterTrying() {
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
    public void shouldBailOnThreadInterrupt() throws Exception {
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
    public void shouldBailOnTermination() throws Exception {
        withPool(4, pool -> {
            Tasks tasks = new Tasks(6, 100);
            AtomicBoolean running = new AtomicBoolean(true);
            TerminationFlag isRunning = running::get;
            final Thread thread = new Thread(() -> tasks.run(t ->
                    ParallelUtil.runWithConcurrency(2, t, isRunning, pool)));

            thread.start();
            running.set(false);
            thread.join();

            assertTrue(tasks.started() <= 2);
            assertTrue(tasks.maxRunning() <= 2);
            assertTrue(tasks.requested() <= 2);
        });
    }

    @Test
    public void shouldWaitThenThrowOnFullThreadpool() throws Exception {
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

    private PrimitiveIntIterable ints(int from, int size) {
        final PrimitiveIntStack stack = new PrimitiveIntStack(size);
        for (int i = 0; i < size; i++) {
            stack.push(i + from);
        }
        return stack;
    }
}
