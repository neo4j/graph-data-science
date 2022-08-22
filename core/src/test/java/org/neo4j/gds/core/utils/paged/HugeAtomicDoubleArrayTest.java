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
package org.neo4j.gds.core.utils.paged;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.gds.mem.MemoryUsage;
import org.opentest4j.AssertionFailedError;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static io.qala.datagen.RandomShortApi.bool;
import static io.qala.datagen.RandomShortApi.integer;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.core.utils.paged.HugeArrays.PAGE_SIZE;

/**
 * Many of the following tests were taken from the AtomicLongArray test from the OpenJDK sources.
 * We also extend RandomizedTest as this class also includes a check that we don't leak threads.
 *
 * @see <a href="https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/test/java/util/concurrent/tck/AtomicLongArrayTest.java">OpenJDK sources for AtomicLongArrayTest.java</a>
 * @see <a href="https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/test/java/util/concurrent/tck/Atomic8Test.java">OpenJDK sources for Atomic8Test.java</a>
 * @see <a href="https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/test/java/util/concurrent/tck/JSR166TestCase.java">OpenJDK sources for JSR166TestCase.java</a>
 * @see <a href="https://hg.openjdk.java.net/jdk/jdk13/file/9e0c80381e32/jdk/test/java/util/concurrent/atomic/LongAdderDemo.java">OpenJDK sources for LongAdderDemo.java</a>
 */
final class HugeAtomicDoubleArrayTest {

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
     * get and set for out of bound indices throw IndexOutOfBoundsException
     */
    @Test
    void testIndexing() {
        testArray(SIZE, array -> {
            for (int index : new int[]{-1, SIZE}) {
                assertThrows(ArrayIndexOutOfBoundsException.class, () -> array.get(index));
                assertThrows(ArrayIndexOutOfBoundsException.class, () -> array.set(index, 1));
                assertThrows(ArrayIndexOutOfBoundsException.class, () -> array.compareAndSet(index, 1, 2));
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
            assertFalse(t.isAlive());
            Assertions.assertEquals(3, array.get(0));
        });
    }

    @Test
    void testCompareAndExchange() {
        testArray(SIZE, array -> {
            for (int i = 0; i < SIZE; i++) {
                array.set(i, 1);
                Assertions.assertEquals(1D, array.compareAndExchange(i, 1, 2));
                Assertions.assertEquals(2D, array.compareAndExchange(i, 2, -4));
                Assertions.assertEquals(-4D, array.get(i));
                Assertions.assertEquals(-4D, array.compareAndExchange(i, -5, 7));
                Assertions.assertEquals(-4D, array.get(i));
                Assertions.assertEquals(-4D, array.compareAndExchange(i, -4, 7));
                Assertions.assertEquals(7D, array.get(i));
            }
        });
    }

    @Test
    void testCompareAndExchangeInMultipleThreads() throws InterruptedException {
        testArray(1, array -> {
            array.set(0, 1);
            Thread t = new Thread(new HugeAtomicDoubleArrayTest.CheckedRunnable() {
                public void realRun() {
                    while (array.compareAndExchange(0, 2, 3) != 2) {
                        Thread.yield();
                    }
                }
            });

            t.start();
            Assertions.assertEquals(1D, array.compareAndExchange(0, 1, 2));
            t.join(LONG_DELAY_MS);
            assertFalse(t.isAlive());
            Assertions.assertEquals(3D, array.get(0));
        });
    }

    private static double addDouble17(double x) { return x + 17; }

    /**
     * getAndUpdate returns previous value and updates result of supplied function
     */
    @Test
    void testAddAndGet() {
        testArray(SIZE, array -> {
            for (int i = 0; i < SIZE; i++) {
                array.set(i, 1);
                array.update(i, HugeAtomicDoubleArrayTest::addDouble17);
                Assertions.assertEquals(18L, array.get(i));
                array.update(i, HugeAtomicDoubleArrayTest::addDouble17);
                Assertions.assertEquals(35L, array.get(i));
            }
        });
    }

    static class Counter extends CheckedRunnable {
        final HugeAtomicDoubleArray array;
        int decs;

        Counter(HugeAtomicDoubleArray array) { this.array = array; }

        public void realRun() {
            for (; ; ) {
                boolean done = true;
                for (int i = 0; i < array.size(); i++) {
                    double v = array.get(i);
                    assertTrue(v >= 0);
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
            assertEquals(c1.decs + c2.decs, SIZE * countdown);
        });
    }

    @Test
    void shouldSetAndGet() {
        testArray(10, array -> {
            int index = integer(2, 8);
            int value = integer(42, 1337);

            array.set(index, value);
            assertEquals(value, array.get(index));
        });
    }

    @Test
    void shouldGetAndAddValuesWithinBoundsForPagedArray() {
        var size = PAGE_SIZE * 2 + 1; // We want an array with three pages
        var index = PAGE_SIZE + 1;    // and look up some index larger than what fits in a single page
        var array = pagedArray(size);
        assertDoesNotThrow(() -> array.getAndAdd(index, 1D));
    }

    @Test
    void shouldAddAndGet() {
        testArray(10, array -> {
            int index = integer(2, 8);
            int value = integer(42, 1337);
            int delta = integer(0, 42);

            array.set(index, value);
            array.getAndAdd(index, delta);

            assertEquals(value + delta, array.get(index));
        });
    }

    @Test
    void shouldReportSize() {
        int size = integer(10, 20);
        testArray(size, array -> assertEquals(size, array.size()));
    }

    @Test
    void shouldFreeMemoryUsed() {
        int size = integer(10, 20);
        long expected = MemoryUsage.sizeOfLongArray(size);
        testArray(size, array -> {
            long freed = array.release();
            assertThat(freed, anyOf(is(expected), is(expected + 24)));
        });
    }

    @Test
    void shouldComputeMemoryEstimation() {
        assertEquals(40, HugeAtomicDoubleArray.memoryEstimation(0L));
        assertEquals(840, HugeAtomicDoubleArray.memoryEstimation(100L));
        assertEquals(800_122_070_368L, HugeAtomicDoubleArray.memoryEstimation(100_000_000_000L));
    }

    @Test
    void shouldFailForNegativeMemRecSize() {
        assertThrows(AssertionError.class, () -> HugeAtomicLongArray.memoryEstimation(-1L));
    }

    @FunctionalInterface
    interface HadaFunction {

        void apply(HugeAtomicDoubleArray array);
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

    private static void casTest(int nthreads, int incs, HugeAtomicDoubleArray a, Executor pool, HadaFunction arrayFn) {
        Phaser phaser = new Phaser(nthreads + 1);
        for (int i = 0; i < nthreads; ++i) {
            pool.execute(new CasTask(a, phaser, incs, arrayFn));
        }
        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndAwaitAdvance();
        long total = (long) nthreads * incs;
        assertEquals(a.get(0), total);
    }

    private static final class CasTask implements Runnable {
        final HugeAtomicDoubleArray adder;
        final Phaser phaser;
        final int incs;
        final HadaFunction arrayFn;
        volatile double result;

        CasTask(
            HugeAtomicDoubleArray adder,
            Phaser phaser,
            int incs,
            HadaFunction arrayFn
        ) {
            this.adder = adder;
            this.phaser = phaser;
            this.incs = incs;
            this.arrayFn = arrayFn;
        }

        public void run() {
            phaser.arriveAndAwaitAdvance();
            HugeAtomicDoubleArray array = adder;
            for (int i = 0; i < incs; ++i) {
                arrayFn.apply(array);
            }
            result = array.get(0);
            phaser.arrive();
        }
    }

    private <E extends Exception> void testArray(int size, ThrowingConsumer<HugeAtomicDoubleArray, E> block) throws E {
        if (bool()) {
            block.accept(singleArray(size));
            block.accept(pagedArray(size));
        } else {
            block.accept(pagedArray(size));
            block.accept(singleArray(size));
        }
    }

    private HugeAtomicDoubleArray singleArray(final int size) {
        return HugeAtomicDoubleArray.newSingleArray(size, DoublePageCreator.passThrough(1));
    }

    private HugeAtomicDoubleArray pagedArray(final int size) {
        return HugeAtomicDoubleArray.newPagedArray(size, DoublePageCreator.passThrough(1));
    }

    /**
     * Fails with message "should throw exception".
     */
    private void shouldThrow() {
        fail("Should throw exception");
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
}
