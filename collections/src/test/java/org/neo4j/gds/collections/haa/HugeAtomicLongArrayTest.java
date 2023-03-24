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
package org.neo4j.gds.collections.haa;

import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.mem.MemoryUsage;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;
import java.util.stream.IntStream;

import static io.qala.datagen.RandomShortApi.bool;
import static io.qala.datagen.RandomShortApi.integer;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Many of the following tests were taken from the AtomicLongArray test from the OpenJDK sources.
 * We also include a check that we don't leak threads.
 *
 * @see <a href="https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/test/java/util/concurrent/tck/AtomicLongArrayTest.java">OpenJDK sources for AtomicLongArrayTest.java</a>
 * @see <a href="https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/test/java/util/concurrent/tck/Atomic8Test.java">OpenJDK sources for Atomic8Test.java</a>
 * @see <a href="https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/test/java/util/concurrent/tck/JSR166TestCase.java">OpenJDK sources for JSR166TestCase.java</a>
 * @see <a href="https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/test/java/util/concurrent/atomic/LongAdderDemo.java">OpenJDK sources for LongAdderDemo.java</a>
 */
final class HugeAtomicLongArrayTest {

    private static final int SIZE = 20;
    private static final long LONG_DELAY_MS = 10_000L;

    /**
     * constructor creates array of given size with all elements zero
     */
    @Test
    void testConstructor() {
        testArray(SIZE, array -> {
            for (int i = 0; i < SIZE; i++) {
                Assertions.assertEquals(0, array.get(i));
            }
        });
    }

    /**
     * constructor with array is of same size and has all elements
     */
    @Test
    void testConstructor2() {
        long[] a = {17L, 3L, -42L, 99L, -7L};
        LongUnaryOperator valueProcuder = i -> a[(int) i % a.length];
        testArray(a.length, valueProcuder, array -> {
            Assertions.assertEquals(a.length, array.size());
            for (int i = 0; i < a.length; i++) {
                Assertions.assertEquals(a[i], array.get(i));
            }
        });
    }

    @Test
    void testIndexing() {
        testArray(SIZE, array -> {
            for (int index : new int[]{-1, SIZE}) {
                assertThatThrownBy(() -> array.get(index)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
                assertThatThrownBy(() -> array.set(index, 1)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
                assertThatThrownBy(() -> array.compareAndSet(
                    index,
                    1,
                    2
                )).isInstanceOf(ArrayIndexOutOfBoundsException.class);
            }
        });
    }

    /**
     * get returns the last value set at index
     */
    @Test
    void testGetSet() {
        testArray(SIZE, array -> {
            for (int i = 0; i < SIZE; i++) {
                array.set(i, 1);
                Assertions.assertEquals(1, array.get(i));
                array.set(i, 2);
                Assertions.assertEquals(2, array.get(i));
                array.set(i, -3);
                Assertions.assertEquals(-3, array.get(i));
            }
        });
    }

    /**
     * compareAndSet succeeds in changing value if equal to expected else fails
     */
    @Test
    void testCompareAndSet() {
        testArray(SIZE, array -> {
            for (int i = 0; i < SIZE; i++) {
                array.set(i, 1);
                Assertions.assertTrue(array.compareAndSet(i, 1, 2));
                Assertions.assertTrue(array.compareAndSet(i, 2, -4));
                Assertions.assertEquals(-4, array.get(i));
                Assertions.assertFalse(array.compareAndSet(i, -5, 7));
                Assertions.assertEquals(-4, array.get(i));
                Assertions.assertTrue(array.compareAndSet(i, -4, 7));
                Assertions.assertEquals(7, array.get(i));
            }
        });
    }

    /**
     * compareAndSet in one thread enables another waiting for value
     * to succeed
     */
    @Test
    void testCompareAndSetInMultipleThreads() throws InterruptedException {
        testArray(1, array -> {
            array.set(0, 1);
            Thread t = new Thread(new CheckedRunnable() {
                public void realRun() {
                    while (!array.compareAndSet(0, 2, 3)) {
                        Thread.yield();
                    }
                }
            });

            t.start();
            Assertions.assertTrue(array.compareAndSet(0, 1, 2));
            t.join(LONG_DELAY_MS);
            assertThat(t.isAlive()).isFalse();
            Assertions.assertEquals(3, array.get(0));
        });
    }

    @Test
    void testCompareAndExchange() {
        testArray(SIZE, array -> {
            for (int i = 0; i < SIZE; i++) {
                array.set(i, 1);
                Assertions.assertEquals(1L, array.compareAndExchange(i, 1, 2));
                Assertions.assertEquals(2L, array.compareAndExchange(i, 2, -4));
                Assertions.assertEquals(-4L, array.get(i));
                Assertions.assertEquals(-4L, array.compareAndExchange(i, -5, 7));
                Assertions.assertEquals(-4L, array.get(i));
                Assertions.assertEquals(-4L, array.compareAndExchange(i, -4, 7));
                Assertions.assertEquals(7L, array.get(i));
            }
        });
    }

    @Test
    void testCompareAndExchangeInMultipleThreads() throws InterruptedException {
        testArray(1, array -> {
            array.set(0, 1);
            Thread t = new Thread(new CheckedRunnable() {
                public void realRun() {
                    while (array.compareAndExchange(0, 2, 3) != 2) {
                        Thread.yield();
                    }
                }
            });

            t.start();
            Assertions.assertEquals(1L, array.compareAndExchange(0, 1, 2));
            t.join(LONG_DELAY_MS);
            assertThat(t.isAlive()).isFalse();
            Assertions.assertEquals(3L, array.get(0));
        });
    }

    private static long addLong17(long x) {return x + 17;}

    /**
     * getAndUpdate returns previous value and updates result of supplied function
     */
    @Test
    void testAddAndGet() {
        testArray(SIZE, array -> {
            for (int i = 0; i < SIZE; i++) {
                array.set(i, 1);
                array.update(i, HugeAtomicLongArrayTest::addLong17);
                Assertions.assertEquals(18L, array.get(i));
                array.update(i, HugeAtomicLongArrayTest::addLong17);
                Assertions.assertEquals(35L, array.get(i));
            }
        });
    }

    @Test
    void testGetAndReplace() {
        testArray(SIZE, array -> {
            for (int i = 0; i < SIZE; i++) {
                array.set(i, 1);
                Assertions.assertEquals(1, array.getAndReplace(i, 42));
                Assertions.assertEquals(42, array.getAndReplace(i, 84));
                Assertions.assertEquals(84, array.getAndReplace(i, -42));
                Assertions.assertEquals(-42, array.get(i));
            }
        });
    }

    static class Counter extends CheckedRunnable {
        final HugeAtomicLongArray array;
        int decs;

        Counter(HugeAtomicLongArray array) {this.array = array;}

        public void realRun() {
            for (; ; ) {
                boolean done = true;
                for (int i = 0; i < array.size(); i++) {
                    long v = array.get(i);
                    assertThat(v).isGreaterThanOrEqualTo(0);
                    if (v != 0) {
                        done = false;
                        if (array.compareAndSet(i, v, v - 1)) {
                            decs++;
                        }
                    }
                }
                if (done) {
                    break;
                }
            }
        }
    }

    /**
     * Multiple threads using same array of counters successfully
     * update a number of times equal to total count
     */
    @Test
    void testCountingInMultipleThreads() throws InterruptedException {
        testArray(SIZE, array -> {
            long countdown = 10000;
            for (int i = 0; i < SIZE; i++) {
                array.set(i, countdown);
            }
            Counter c1 = new Counter(array);
            Counter c2 = new Counter(array);
            Thread t1 = newStartedThread(c1);
            Thread t2 = newStartedThread(c2);
            t1.join();
            t2.join();
            assertThat(c1.decs + c2.decs).isEqualTo(SIZE * countdown);
        });
    }

    @Test
    void shouldSetAndGet() {
        testArray(10, array -> {
            int index = integer(2, 8);
            int value = integer(42, 1337);

            array.set(index, value);
            assertThat(array.get(index)).isEqualTo(value);
        });
    }

    @Test
    void shouldAddAndGet() {
        testArray(10, array -> {
            int index = integer(2, 8);
            int value = integer(42, 1337);
            int delta = integer(0, 42);

            array.set(index, value);
            array.getAndAdd(index, delta);

            assertThat(array.get(index)).isEqualTo(value + delta);
        });
    }

    @Test
    void shouldReportSize() {
        int size = integer(10, 20);
        testArray(size, array -> assertThat(array.size()).isEqualTo(size));
    }

    @Test
    void shouldFreeMemoryUsed() {
        int size = integer(10, 20);
        long expected = MemoryUsage.sizeOfLongArray(size);
        testArray(size, array -> {
            long freed = array.release();
            assertThat(freed).matches(v -> v == expected || v == expected + 24);
        });
    }

    @ParameterizedTest
    @CsvSource({
        "0, 40",
        "100, 840",
        "100_000_000_000, 800_122_070_368",
    })
    void shouldComputeMemoryEstimation(long size, long estimation) {
        assertThat(HugeAtomicLongArray.memoryEstimation(size)).isEqualTo(estimation);
    }

    @Test
    void shouldFailForNegativeMemRecSize() {
        assertThrows(AssertionError.class, () -> HugeAtomicLongArray.memoryEstimation(-1L));
    }

    @Test
    void testSetAll() {
        var pool = Executors.newCachedThreadPool();
        try {
            int nthreads = Runtime.getRuntime().availableProcessors() * 2;
            var arraySize = 42_1337;
            var phaser = new Phaser(nthreads + 1);
            var aa = pagedArray(arraySize); // 1 page
            var tasks = new ArrayList<GetTask>();
            aa.setAll(42);
            for (int i = 0; i < nthreads; ++i) {
                var t = new GetTask(aa, phaser);
                tasks.add(t);
                pool.execute(t);
            }
            phaser.arriveAndAwaitAdvance();

            tasks.forEach(t -> assertThat(t.result).isEqualTo(42 * arraySize));

        } finally {
            pool.shutdown();
        }
    }

    private static final class GetTask implements Runnable {
        private final HugeAtomicLongArray array;
        private final Phaser phaser;
        volatile long result;

        private GetTask(HugeAtomicLongArray array, Phaser phaser) {
            this.array = array;
            this.phaser = phaser;
        }

        @Override
        public void run() {
            for (int i = 0; i < array.size(); i++) {
                result += array.get(i);
            }
            phaser.arrive();
        }
    }

    @FunctionalInterface
    interface HalaFunction {

        void apply(HugeAtomicLongArray array);
    }

    @Test
    void testUpdateParallel() {
        int incsPerThread = 10_000;
        int ncpu = Runtime.getRuntime().availableProcessors();
        int maxThreads = ncpu * 2;
        ExecutorService pool = Executors.newCachedThreadPool();

        try {
            testArray(1, array -> {
                for (int i = 1; i <= maxThreads; i <<= 1) {
                    array.set(0, 0);
                    casTest(i, incsPerThread, array, pool, a -> a.update(0, x -> x + 1));
                }
            });
        } finally {
            pool.shutdown();
            LockSupport.parkNanos(MILLISECONDS.toNanos(100));
        }
    }

    @Test
    void testGetAndAddIsWithinBoundsForPagedArray() {
        var size = HugeAtomicLongArraySon.Paged.PAGE_SIZE * 2 + 1; // We want an array with three pages
        var index = HugeAtomicLongArraySon.Paged.PAGE_SIZE + 1;    // and look up some index larger than what fits in a single page
        var array = pagedArray(size);

        array.getAndAdd(index, 1);
    }

    @Test
    void testGetAndAddParallel() {
        int incsPerThread = 10_000;
        int ncpu = Runtime.getRuntime().availableProcessors();
        int maxThreads = ncpu * 2;
        ExecutorService pool = Executors.newCachedThreadPool();

        try {
            testArray(1, array -> {
                for (int i = 1; i <= maxThreads; i <<= 1) {
                    array.set(0, 0);
                    casTest(i, incsPerThread, array, pool, a -> a.getAndAdd(0, 1));
                }
            });
        } finally {
            pool.shutdown();
            LockSupport.parkNanos(MILLISECONDS.toNanos(100));
        }
    }

    private static void casTest(
        int nthreads,
        int incs,
        HugeAtomicLongArray array,
        Executor pool,
        HalaFunction arrayFn
    ) {
        Phaser phaser = new Phaser(nthreads + 1);
        for (int i = 0; i < nthreads; ++i) {
            pool.execute(new CasTask(array, phaser, incs, arrayFn));
        }
        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndAwaitAdvance();
        long total = (long) nthreads * incs;
        assertThat(array.get(0)).isEqualTo(total);
    }

    private static final class CasTask implements Runnable {
        final HugeAtomicLongArray adder;
        final Phaser phaser;
        final int incs;
        volatile long result;
        final HalaFunction arrayFn;

        CasTask(HugeAtomicLongArray adder, Phaser phaser, int incs, HalaFunction arrayFn) {
            this.adder = adder;
            this.phaser = phaser;
            this.incs = incs;
            this.arrayFn = arrayFn;
        }

        public void run() {
            phaser.arriveAndAwaitAdvance();
            HugeAtomicLongArray array = adder;
            for (int i = 0; i < incs; ++i) {
                arrayFn.apply(array);
            }
            result = array.get(0);
            phaser.arrive();
        }
    }

    private void testArray(int size, ThrowingConsumer<HugeAtomicLongArray> block) {
        if (bool()) {
            block.accept(singleArray(size));
            block.accept(pagedArray(size));
        } else {
            block.accept(pagedArray(size));
            block.accept(singleArray(size));
        }
    }

    private void testArray(int size, LongUnaryOperator valueProducer, Consumer<HugeAtomicLongArray> block) {
        if (bool()) {
            block.accept(singleArray(size, valueProducer));
            block.accept(pagedArray(size, valueProducer));
        } else {
            block.accept(pagedArray(size, valueProducer));
            block.accept(singleArray(size, valueProducer));
        }
    }

    private HugeAtomicLongArray singleArray(final int size) {
        return singleArray(size, null);
    }

    private HugeAtomicLongArray singleArray(final int size, final LongUnaryOperator valueProducer) {

        return HugeAtomicLongArraySon.Single.of(size, new PageCreator(valueProducer));
    }

    private HugeAtomicLongArray pagedArray(final int size) {
        return pagedArray(size, null);
    }

    private HugeAtomicLongArray pagedArray(final int size, final LongUnaryOperator valueProducer) {
        return HugeAtomicLongArray.of(size, new PageCreator(valueProducer));
    }

    /**
     * Returns a new started daemon Thread running the given runnable.
     */
    private Thread newStartedThread(Runnable runnable) {
        Thread t = new Thread(runnable);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * The first exception encountered if any threadAssertXXX method fails.
     */
    private static final AtomicReference<Throwable> threadFailure
        = new AtomicReference<>(null);

    /**
     * Records an exception so that it can be rethrown later in the test
     * harness thread, triggering a test case failure.  Only the first
     * failure is recorded; subsequent calls to this method from within
     * the same test have no effect.
     */
    private static void threadRecordFailure(Throwable t) {
        threadFailure.compareAndSet(null, t);
    }

    /**
     * Records the given exception using {@link #threadRecordFailure},
     * then rethrows the exception, wrapping it in an
     * AssertionFailedError if necessary.
     */
    private static void threadUnexpectedException(Throwable t) {
        threadRecordFailure(t);
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            throw new AssertionFailedError("unexpected exception: " + t, t);
        }
    }

    private abstract static class CheckedRunnable implements Runnable {
        protected abstract void realRun() throws Throwable;

        public final void run() {
            try {
                realRun();
            } catch (Throwable fail) {
                threadUnexpectedException(fail);
            }
        }
    }

    private static class PageCreator implements org.neo4j.gds.collections.haa.PageCreator.LongPageCreator {

        private final LongUnaryOperator generator;

        PageCreator(LongUnaryOperator generator) {this.generator = generator;}

        @Override
        public void fill(long[][] pages, int lastPageSize, int pageShift) {
            int lastPageIndex = pages.length - 1;
            int pageSize = 1 << pageShift;

            IntStream.range(0, lastPageIndex).forEach(idx -> {
                pages[idx] = new long[pageSize];
                long base = ((long) idx) << pageShift;
                fillPage(pages[idx], base);
            });

            pages[lastPageIndex] = new long[lastPageSize];
        }

        @Override
        public void fillPage(long[] page, long base) {
            if (generator != null) {
                for (var i = 0; i < page.length; i++) {
                    page[i] = generator.applyAsLong(i + base);
                }
            }
        }
    }
}
