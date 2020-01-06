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
package org.neo4j.graphalgo.core.utils.paged;

import org.junit.jupiter.api.Test;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.opentest4j.AssertionFailedError;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

import static io.qala.datagen.RandomShortApi.bool;
import static io.qala.datagen.RandomShortApi.integer;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Many of the following tests were taken from the AtomicLongArray test from the OpenJDK sources.
 * We also extend RandomizedTest as this class also includes a check that we don't leak threads.
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
        testArray(SIZE, aa -> {
            for (int i = 0; i < SIZE; i++) {
                assertEquals(0, aa.get(i));
            }
        });
    }

    /**
     * constructor with array is of same size and has all elements
     */
    @Test
    void testConstructor2() {
        long[] a = {17L, 3L, -42L, 99L, -7L};
        testArray(a.length, i -> a[(int) i], aa -> {
            assertEquals(a.length, aa.size());
            for (int i = 0; i < a.length; i++) {
                assertEquals(a[i], aa.get(i));
            }
        });
    }

    /**
     * get and set for out of bound indices throw IndexOutOfBoundsException
     */
    @Test
    void testIndexing() {
        testArray(SIZE, aa -> {
            for (int index : new int[]{-1, SIZE}) {
                try {
                    aa.get(index);
                    shouldThrow();
                } catch (AssertionError ignored) {
                }
                try {
                    aa.set(index, 1);
                    shouldThrow();
                } catch (AssertionError ignored) {
                }
                try {
                    aa.compareAndSet(index, 1, 2);
                    shouldThrow();
                } catch (AssertionError ignored) {
                }
            }
        });
    }

    /**
     * get returns the last value set at index
     */
    @Test
    void testGetSet() {
        testArray(SIZE, aa -> {
            for (int i = 0; i < SIZE; i++) {
                aa.set(i, 1);
                assertEquals(1, aa.get(i));
                aa.set(i, 2);
                assertEquals(2, aa.get(i));
                aa.set(i, -3);
                assertEquals(-3, aa.get(i));
            }
        });
    }

    /**
     * compareAndSet succeeds in changing value if equal to expected else fails
     */
    @Test
    void testCompareAndSet() {
        testArray(SIZE, aa -> {
            for (int i = 0; i < SIZE; i++) {
                aa.set(i, 1);
                assertTrue(aa.compareAndSet(i, 1, 2));
                assertTrue(aa.compareAndSet(i, 2, -4));
                assertEquals(-4, aa.get(i));
                assertFalse(aa.compareAndSet(i, -5, 7));
                assertEquals(-4, aa.get(i));
                assertTrue(aa.compareAndSet(i, -4, 7));
                assertEquals(7, aa.get(i));
            }
        });
    }

    /**
     * compareAndSet in one thread enables another waiting for value
     * to succeed
     */
    @Test
    void testCompareAndSetInMultipleThreads() throws InterruptedException {
        testArray(1, a -> {
            a.set(0, 1);
            Thread t = new Thread(new CheckedRunnable() {
                public void realRun() {
                    while (!a.compareAndSet(0, 2, 3)) {
                        Thread.yield();
                    }
                }
            });

            t.start();
            assertTrue(a.compareAndSet(0, 1, 2));
            t.join(LONG_DELAY_MS);
            assertFalse(t.isAlive());
            assertEquals(3, a.get(0));
        });

    }

    private static long addLong17(long x) { return x + 17; }

    /**
     * getAndUpdate returns previous value and updates result of supplied function
     */
    @Test
    void testAddAndGet() {
        testArray(SIZE, aa -> {
            for (int i = 0; i < SIZE; i++) {
                aa.set(i, 1);
                aa.update(i, HugeAtomicLongArrayTest::addLong17);
                assertEquals(18L, aa.get(i));
                aa.update(i, HugeAtomicLongArrayTest::addLong17);
                assertEquals(35L, aa.get(i));
            }
        });
    }

    static class Counter extends CheckedRunnable {
        final HugeAtomicLongArray aa;
        int decs;

        Counter(HugeAtomicLongArray a) { aa = a; }

        public void realRun() {
            for (; ; ) {
                boolean done = true;
                for (int i = 0; i < aa.size(); i++) {
                    long v = aa.get(i);
                    assertTrue(v >= 0);
                    if (v != 0) {
                        done = false;
                        if (aa.compareAndSet(i, v, v - 1)) {
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
        testArray(SIZE, aa -> {
            long countdown = 10000;
            for (int i = 0; i < SIZE; i++) {
                aa.set(i, countdown);
            }
            Counter c1 = new Counter(aa);
            Counter c2 = new Counter(aa);
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
        assertEquals(40, HugeAtomicLongArray.memoryEstimation(0L));
        assertEquals(840, HugeAtomicLongArray.memoryEstimation(100L));
        assertEquals(800_122_070_368L, HugeAtomicLongArray.memoryEstimation(100_000_000_000L));
    }

    @Test
    void shouldFailForNegativeMemRecSize() {
        assertThrows(AssertionError.class, () -> HugeAtomicLongArray.memoryEstimation(-1L));
    }

    @Test
    void testAdder() {
        int incsPerThread = 10_000;
        int ncpu = Runtime.getRuntime().availableProcessors();
        int maxThreads = ncpu * 2;
        ExecutorService pool = Executors.newCachedThreadPool();

        try {
            testArray(1, aa -> {
                for (int i = 1; i <= maxThreads; i <<= 1) {
                    aa.set(0, 0);
                    casTest(i, incsPerThread, aa, pool);
                }
            });
        } finally {
            pool.shutdown();
            LockSupport.parkNanos(MILLISECONDS.toNanos(100));
        }
    }

    private static void casTest(int nthreads, int incs, HugeAtomicLongArray a, Executor pool) {
        Phaser phaser = new Phaser(nthreads + 1);
        for (int i = 0; i < nthreads; ++i) {
            pool.execute(new CasTask(a, phaser, incs));
        }
        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndAwaitAdvance();
        long total = (long) nthreads * incs;
        assertEquals(a.get(0), total);
    }

    private static final class CasTask implements Runnable {
        final HugeAtomicLongArray adder;
        final Phaser phaser;
        final int incs;
        volatile long result;

        CasTask(HugeAtomicLongArray adder, Phaser phaser, int incs) {
            this.adder = adder;
            this.phaser = phaser;
            this.incs = incs;
        }

        public void run() {
            phaser.arriveAndAwaitAdvance();
            HugeAtomicLongArray a = adder;
            for (int i = 0; i < incs; ++i) {
                a.update(0, x -> x + 1);
            }
            result = a.get(0);
            phaser.arrive();
        }
    }

    private <E extends Exception> void testArray(int size, ThrowingConsumer<HugeAtomicLongArray, E> block) throws E {
        if (bool()) {
            block.accept(singleArray(size));
            block.accept(pagedArray(size));
        } else {
            block.accept(pagedArray(size));
            block.accept(singleArray(size));
        }
    }

    private void testArray(int size, LongUnaryOperator gen, Consumer<HugeAtomicLongArray> block) {
        if (bool()) {
            block.accept(singleArray(size, gen));
            block.accept(pagedArray(size, gen));
        } else {
            block.accept(pagedArray(size, gen));
            block.accept(singleArray(size, gen));
        }
    }

    private HugeAtomicLongArray singleArray(final int size) {
        return HugeAtomicLongArray.newSingleArray(size, null, AllocationTracker.EMPTY);
    }

    private HugeAtomicLongArray singleArray(final int size, final LongUnaryOperator gen) {
        return HugeAtomicLongArray.newSingleArray(size, gen, AllocationTracker.EMPTY);
    }

    private HugeAtomicLongArray pagedArray(final int size) {
        return HugeAtomicLongArray.newPagedArray(size, null, AllocationTracker.EMPTY);
    }

    private HugeAtomicLongArray pagedArray(final int size, final LongUnaryOperator gen) {
        return HugeAtomicLongArray.newPagedArray(size, gen, AllocationTracker.EMPTY);
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
        t.printStackTrace();
        threadFailure.compareAndSet(null, t);
    }

    /**
     * Records the given exception using {@link #threadRecordFailure},
     * then rethrows the exception, wrapping it in an
     * AssertionFailedError if necessary.
     */
    private static void threadUnexpectedException(Throwable t) {
        threadRecordFailure(t);
        t.printStackTrace();
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
