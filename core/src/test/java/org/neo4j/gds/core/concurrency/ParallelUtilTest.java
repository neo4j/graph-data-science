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
package org.neo4j.gds.core.concurrency;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongCollections;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.TestSupport.assertTransactionTermination;
import static org.neo4j.gds.core.concurrency.ParallelUtil.parallelStream;
import static org.neo4j.gds.core.concurrency.ParallelUtil.parallelStreamConsume;
import static org.neo4j.gds.utils.ExceptionUtil.throwIfUnchecked;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class ParallelUtilTest {

    @ValueSource(ints = {1, 2, 4, 8, 16, 27, 32, 53, 64})
    @ParameterizedTest
    void shouldParallelizeStreams(int concurrency) {
        long firstNum = 1;
        long lastNum = 1_000_000;

        List<Long> list = LongStream.rangeClosed(firstNum, lastNum).boxed().collect(Collectors.toList());

        ForkJoinPool commonPool = ForkJoinPool.commonPool();
        Stream<Long> stream = list.stream();

        long actualTotal = parallelStream(stream, concurrency, (s) -> {
            assertTrue(s.isParallel());
            Thread thread = Thread.currentThread();
            assertTrue(thread instanceof ForkJoinWorkerThread);
            assertThat(thread.getName()).contains("gds-forkjoin");
            ForkJoinPool threadPool = ((ForkJoinWorkerThread) thread).getPool();
            assertEquals(concurrency, threadPool.getParallelism());
            assertNotSame(threadPool, commonPool);

            return s.reduce(0L, Long::sum);
        });

        assertEquals((lastNum + firstNum) * lastNum / 2, actualTotal);
    }

    @ValueSource(ints = {1, 2, 3, 4})
    @ParameterizedTest
    void shouldParallelizeStreamsWithLimitedConcurrency(int concurrency) {
        LongStream data = LongStream.range(0, 100_000);
        Long result = parallelStream(data, concurrency, (s) -> {
            assertTrue(s.isParallel());
            Thread thread = Thread.currentThread();
            assertTrue(thread instanceof ForkJoinWorkerThread);
            ForkJoinPool threadPool = ((ForkJoinWorkerThread) thread).getPool();
            assertEquals(concurrency, threadPool.getParallelism());

            return s.reduce(0L, Long::sum);
        });

        assertEquals((99_999L * 100_000L / 2), result);
    }

    @ValueSource(ints = {1, 2, 3, 4})
    @ParameterizedTest
    void shouldParallelizeAndConsumeStreamsWithLimitedConcurrency(int concurrency) {
        LongStream data = LongStream.range(0, 100_000);
        parallelStreamConsume(data, concurrency, (s) -> {
            assertTrue(s.isParallel());
            Thread thread = Thread.currentThread();
            assertTrue(thread instanceof ForkJoinWorkerThread);
            ForkJoinPool threadPool = ((ForkJoinWorkerThread) thread).getPool();
            assertEquals(concurrency, threadPool.getParallelism());

            long result = s.reduce(0L, Long::sum);
            assertEquals((99_999L * 100_000L / 2), result);
        });
    }

    @Test
    void shouldTakeBaseStreams() {
        double[] data = {1.0, 2.5, 3.14};

        double sum = parallelStream(Arrays.stream(data), 4, DoubleStream::sum);

        assertEquals(1.0 + 2.5 + 3.14, sum);
    }

    @Test
    void threadSizeShouldDivideByBatchsize() {
        Assertions.assertEquals(42, ParallelUtil.threadCount(1337, 1337 * 42));
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
    void batchingShouldCatenatePartitions() {
        int minBatchSize = 21;
        int maxConcurrency = 8;
        int nodeCount = 1337;

        String params = formatWithLocale(
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
            RunWithConcurrency.builder()
                .concurrency(concurrency)
                .tasks(ts)
                .executor(pool)
                .run();
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
                t -> RunWithConcurrency.builder()
                    .concurrency(8)
                    .tasks(t)
                    .executor(null)
                    .run(),
                // terminated pool
                t -> RunWithConcurrency.builder()
                    .concurrency(8)
                    .tasks(t)
                    .executor(deadPool)
                    .run(),
                // single task
                t -> RunWithConcurrency.builder()
                    .concurrency(8)
                    .tasks(t.sized(1))
                    .executor(pool)
                    .run(),
                // concurrency = 1
                t -> RunWithConcurrency.builder()
                    .concurrency(1)
                    .tasks(t)
                    .executor(pool)
                    .run(),
                // concurrency = 0
                t -> RunWithConcurrency.builder()
                    .concurrency(0)
                    .tasks(t)
                    .executor(pool)
                    .run()
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
            tasks.run(t -> RunWithConcurrency.builder()
                .concurrency(2)
                .tasks(t)
                .executor(pool)
                .run());
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
            tasks.run(t -> RunWithConcurrency.builder()
                .concurrency(4)
                .tasks(t)
                .maxWaitRetries(100)
                .executor(pool)
                .run());
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("Attempted to submit tasks");
        }
        assertEquals(0, tasks.started());
        assertEquals(0, tasks.maxRunning());
        assertEquals(1, tasks.requested());
    }

    static final class RemainingAssertions {
        final Object expected;
        final Object actual;

        RemainingAssertions(Object expected, Object actual) {
            this.expected = expected;
            this.actual = actual;
        }
    }

    @Test
    void shouldBailOnThreadInterrupt() {
        withPool(4, pool -> {
            Tasks tasks = new Tasks(6, 10);
            Collection<RemainingAssertions> assertions = new ConcurrentLinkedQueue<>();
            final Thread thread = new Thread(() -> tasks.run(t -> RunWithConcurrency.builder()
                .concurrency(2)
                .tasks(t)
                .executor(pool)
                .run()));
            thread.setUncaughtExceptionHandler((t, e) -> {
                assertions.add(new RemainingAssertions(
                    "java.lang.InterruptedException", e.getMessage()
                ));
                assertions.add(new RemainingAssertions(
                    InterruptedException.class, e.getCause().getClass()
                ));
            });

            thread.start();
            thread.interrupt();
            thread.join();

            assertTrue(tasks.started() <= 2);
            assertTrue(tasks.maxRunning() <= 2);
            assertTrue(tasks.requested() <= 2);

            for (RemainingAssertions assertion : assertions) {
                assertEquals(assertion.expected, assertion.actual);
            }
        });
    }

    @Test
    void shouldBailOnTermination() {
        assertTransactionTermination(
            () -> withPool(4, pool -> {
                Tasks tasks = new Tasks(6, 100);
                AtomicReference<Throwable> thrownException = new AtomicReference<>();
                AtomicBoolean running = new AtomicBoolean(true);
                TerminationFlag isRunning = running::get;
                var thread = Pools.newThread(() -> tasks.run(t ->
                    RunWithConcurrency.builder()
                        .concurrency(2)
                        .tasks(t)
                        .terminationFlag(isRunning)
                        .executor(pool)
                        .run()
                ));

                thread.setUncaughtExceptionHandler((t, e) -> thrownException.set(e));
                thread.start();
                running.set(false);
                thread.join();

                assertTrue(tasks.started() <= 2);
                assertTrue(tasks.maxRunning() <= 2);
                assertTrue(tasks.requested() <= 2);

                if (thrownException.get() != null) {
                    throw thrownException.get();
                }
            })
        );
    }

    @Test
    void shouldWaitThenThrowOnFullThreadpool() {
        ThreadPoolExecutor pool = mock(ThreadPoolExecutor.class);
        when(pool.getActiveCount()).thenReturn(Integer.MAX_VALUE);
        Tasks tasks = new Tasks(5, 10);
        try {
            tasks.run(t -> RunWithConcurrency.builder()
                .concurrency(4)
                .tasks(t)
                .maxWaitRetries(10)
                .waitTime(5, TimeUnit.MILLISECONDS)
                .executor(pool)
                .run());
        } catch (IllegalThreadStateException e) {
            assertThat(e.getMessage()).contains("Attempted to submit tasks");
        }
        assertEquals(0, tasks.started());
        assertEquals(0, tasks.maxRunning());
        assertEquals(1, tasks.requested());
        verify(pool, times(11)).getActiveCount();
    }

    @RepeatedTest(100)
    void shouldKeepTrackOfAllErrors() {
        var counter = new AtomicInteger(0);
        var tasks = List.<Runnable>of(
            () -> {
                counter.incrementAndGet();
                throw new RuntimeException("bubu");
            },
            counter::incrementAndGet
        );
        assertThatThrownBy(() -> RunWithConcurrency.builder()
            .concurrency(4)
            .tasks(tasks)
            .run())
            .isInstanceOf(RuntimeException.class)
            .hasMessage("bubu");
        assertThat(counter.get()).isEqualTo(2);
    }

    @Test
    void shouldCollectExceptionsFromFailingTasks() {
        AtomicInteger successfulTasks = new AtomicInteger();

        Set<Runnable> tasks = new HashSet<>();
        for (int i = 0; i < 42; i++) { // 42 succeeding tasks
            tasks.add(successfulTasks::incrementAndGet);
        }
        // funky bounds because we checksum later
        for (int i = 1; i <= 23; i++) { // 23 failing tasks
            int j = i;
            tasks.add(() -> {
                throw new RuntimeException("pu exception" + j);
            });
        }

        try {
            RunWithConcurrency.builder()
                .concurrency(7)
                .tasks(tasks)
                .run();

            fail("it should have thrown");
        } catch (RuntimeException oneOfTheTwentyThree) {
            assertThat(oneOfTheTwentyThree.getMessage()).startsWith("pu exception");
            assertThat(oneOfTheTwentyThree.getSuppressed().length).isEqualTo(22); // because one holds the other ones

            Stream<Throwable> allTwentyThreeExceptions = Stream.concat(
                Stream.of(oneOfTheTwentyThree),
                Arrays.stream(oneOfTheTwentyThree.getSuppressed())
            );

            int checkSumOnFailedTasks = allTwentyThreeExceptions
                .map(throwable -> throwable.getMessage().substring(12))
                .mapToInt(Integer::valueOf)
                .sum();
            assertThat(checkSumOnFailedTasks).isEqualTo(23 * (23 + 1) / 2); // 276 == sum(1..23)
        }

        assertThat(successfulTasks.get()).isEqualTo(42);
    }

    private static void withPool(
            int nThreads,
            ThrowingConsumer<ExecutorService, ? extends Throwable> block) {
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        try {
            block.accept(pool);
        } catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
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

    static final class Tasks extends AbstractCollection<Runnable> {
        private final AtomicInteger started;
        private final AtomicInteger running;
        private final AtomicInteger requested;
        private final LongAccumulator maxRunning;
        private int size;
        private final long parkNanos;

        Tasks(int size) {
            this.size = size;
            started = new AtomicInteger();
            running = new AtomicInteger();
            requested = new AtomicInteger();
            maxRunning = new LongAccumulator(Long::max, Long.MIN_VALUE);
            parkNanos = 0;
        }

        Tasks(int size, int runtimeMillis) {
            this.size = size;
            started = new AtomicInteger();
            running = new AtomicInteger();
            requested = new AtomicInteger();
            maxRunning = new LongAccumulator(Long::max, Long.MIN_VALUE);
            parkNanos = TimeUnit.MILLISECONDS.toNanos(runtimeMillis);
        }

        @Override
        public Iterator<Runnable> iterator() {
            started.set(0);
            running.set(0);
            requested.set(0);
            maxRunning.reset();
            return new TrackingIterator<>(IntStream.range(0, size).mapToObj($ -> newTask()).iterator());
        }

        @Override
        public int size() {
            return size;
        }

        Tasks sized(int newSize) {
            size = newSize;
            return this;
        }

        int started() {
            return started.get();
        }

        int maxRunning() {
            return (int) maxRunning.get();
        }

        int requested() {
            return requested.get();
        }

        void run(Consumer<Tasks> block) {
            block.accept(this);
        }

        private Runnable newTask() {
            return () -> {
                maxRunning.accumulate(running.incrementAndGet());
                started.incrementAndGet();
                LockSupport.parkNanos(parkNanos);
                running.decrementAndGet();
            };
        }

        private final class TrackingIterator<T> implements Iterator<T> {
            private final Iterator<T> it;

            private TrackingIterator(Iterator<T> it) {
                this.it = it;
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                requested.incrementAndGet();
                return it.next();
            }
        }
    }
}
