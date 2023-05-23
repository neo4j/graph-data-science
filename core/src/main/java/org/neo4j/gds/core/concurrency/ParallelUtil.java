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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionConsumer;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.utils.CloseableThreadLocal;
import org.neo4j.gds.utils.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ParallelUtil {

    public static final int DEFAULT_BATCH_SIZE = 10_000;

    // prevent instantiation of factory
    private ParallelUtil() {}

    /**
     * Executes the given function in parallel on the given {@link BaseStream}, using a FJ pool of the requested size.
     * The concurrency value is assumed to already be validated towards the edition limitation.
     */
    public static <T extends BaseStream<?, T>, R> R parallelStream(T data, int concurrency, Function<T, R> fn) {
        ForkJoinPool pool = Pools.createForkJoinPool(concurrency);
        try {
            return pool.submit(() -> fn.apply(data.parallel())).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Executes the given function in parallel on the given {@link BaseStream}, using a FJ pool of the requested size.
     * The concurrency value is assumed to already be validated towards the edition limitation.
     */
    public static <T extends BaseStream<?, T>> void parallelStreamConsume(
        T data,
        int concurrency,
        TerminationFlag terminationFlag,
        Consumer<T> consumer
    ) {
        parallelStream(data, concurrency, (Function<T, Void>) t -> {
            terminationFlag.assertRunning();
            consumer.accept(t);
            return null;
        });
    }

    public static <T extends BaseStream<?, T>> void parallelStreamConsume(
        T data,
        int concurrency,
        Consumer<T> consumer
    ) {
        parallelStreamConsume(data, concurrency, TerminationFlag.RUNNING_TRUE, consumer);
    }

    public static void parallelForEachNode(long nodeCount, int concurrency, LongConsumer consumer) {
        parallelForEachNode(nodeCount, concurrency, TerminationFlag.RUNNING_TRUE, consumer);
    }

    public static void parallelForEachNode(
        long nodeCount,
        int concurrency,
        TerminationFlag terminationFlag,
        LongConsumer consumer
    ) {
        parallelStreamConsume(
            LongStream.range(0, nodeCount),
            concurrency,
            terminationFlag,
            stream -> stream.forEach(consumer)
        );
    }

    /**
     * This method is useful, when |partitions| >> concurrency as we only create a single consumer per thread.
     * Compared to parallelForEachNode, thread local state does not need to be resolved for each node but only per partition.
     */
    public static <P extends Partition> void parallelPartitionsConsume(
        RunWithConcurrency.Builder runnerBuilder,
        Stream<P> partitions,
        Supplier<PartitionConsumer<P>> taskSupplier
    ) {
        try (
            var localConsumer = CloseableThreadLocal.withInitial(taskSupplier);
        ) {
            var taskStream = partitions.map(partition -> (Runnable) () -> localConsumer.get().consume(partition));
            runnerBuilder.tasks(taskStream);
            runnerBuilder.run();
        }

    }

    /**
     * @return the number of threads required to compute elementCount with the given batchSize
     */
    public static int threadCount(final int batchSize, final int elementCount) {
        return Math.toIntExact(threadCount((long) batchSize, elementCount));
    }

    public static long threadCount(final long batchSize, final long elementCount) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
        if (batchSize >= elementCount) {
            return 1;
        }
        return BitUtil.ceilDiv(elementCount, batchSize);
    }

    /**
     * @return a batch size, so that {@code nodeCount} is equally divided by {@code concurrency}
     *     but no smaller than {@code minBatchSize}.
     */
    public static int adjustedBatchSize(
        final int nodeCount,
        int concurrency,
        final int minBatchSize
    ) {
        if (concurrency <= 0) {
            concurrency = nodeCount;
        }
        int targetBatchSize = threadCount(concurrency, nodeCount);
        return Math.max(minBatchSize, targetBatchSize);
    }

    /**
     * @return a batch size, so that {@code nodeCount} is equally divided by {@code concurrency}
     *     but no smaller than {@link #DEFAULT_BATCH_SIZE}.
     * @see #adjustedBatchSize(int, int, int)
     */
    public static int adjustedBatchSize(
        final int nodeCount,
        final int concurrency
    ) {
        return adjustedBatchSize(nodeCount, concurrency, DEFAULT_BATCH_SIZE);
    }

    /**
     * @return a batch size, so that {@code nodeCount} is equally divided by {@code concurrency}
     *     but no smaller than {@code minBatchSize}.
     * @see #adjustedBatchSize(int, int, int)
     */
    public static long adjustedBatchSize(
        final long nodeCount,
        int concurrency,
        final long minBatchSize
    ) {
        if (concurrency <= 0) {
            concurrency = (int) Math.min(nodeCount, Integer.MAX_VALUE);
        }
        long targetBatchSize = threadCount(concurrency, nodeCount);
        return Math.max(minBatchSize, targetBatchSize);
    }

    /**
     * @return a batch size, so that {@code nodeCount} is equally divided by {@code concurrency}
     *     but no smaller than {@code minBatchSize} and no larger than {@code maxBatchSize}.
     * @see #adjustedBatchSize(long, int, long)
     */
    public static long adjustedBatchSize(
        final long nodeCount,
        final int concurrency,
        final long minBatchSize,
        final long maxBatchSize
    ) {
        return Math.min(maxBatchSize, adjustedBatchSize(nodeCount, concurrency, minBatchSize));
    }

    /**
     * @return a batch size, that is
     *     1) at least {@code batchSize}
     *     2) a power of two
     *     3) divides {@code nodeCount} into int-sized chunks.
     */
    public static long adjustedBatchSize(final long nodeCount, long batchSize) {
        if (batchSize <= 0L) {
            batchSize = 1L;
        }
        batchSize = BitUtil.nextHighestPowerOfTwo(batchSize);
        while (((nodeCount + batchSize + 1L) / batchSize) > (long) Integer.MAX_VALUE) {
            batchSize = batchSize << 1;
        }
        return batchSize;
    }

    public static boolean canRunInParallel(@Nullable ExecutorService executor) {
        return executor != null && !(executor.isShutdown() || executor.isTerminated());
    }

    public static void readParallel(
        final int concurrency,
        final long size,
        final ExecutorService executor,
        final BiLongConsumer task
    ) {

        long batchSize = threadCount(concurrency, size);
        if (!canRunInParallel(executor) || concurrency == 1) {
            for (long start = 0L; start < size; start += batchSize) {
                long end = Math.min(size, start + batchSize);
                task.apply(start, end);
            }
        } else {
            Collection<Runnable> threads = new ArrayList<>(concurrency);
            for (long start = 0L; start < size; start += batchSize) {
                long end = Math.min(size, start + batchSize);
                final long finalStart = start;
                threads.add(() -> task.apply(finalStart, end));
            }
            run(threads, executor);
        }
    }

    public static Collection<Runnable> tasks(
        final int concurrency,
        final Supplier<? extends Runnable> newTask
    ) {
        final Collection<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(newTask.get());
        }
        return tasks;
    }

    public static Collection<Runnable> tasks(
        final int concurrency,
        final IntFunction<? extends Runnable> newTask
    ) {
        final Collection<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(newTask.apply(i));
        }
        return tasks;
    }

    /**
     * Runs a single task and waits until it's finished.
     */
    public static void run(Runnable task, ExecutorService executor) {
        awaitTermination(Collections.singleton(executor.submit(task)));
    }

    /**
     * Runs a collection of {@link Runnable}s in parallel for their side-effects.
     * The level of parallelism is defined by the given executor.
     * <p>
     * This is similar to {@link ExecutorService#invokeAll(Collection)},
     * except that all Exceptions thrown by any task are chained together.
     */
    public static void run(
        final Collection<? extends Runnable> tasks,
        final ExecutorService executor
    ) {
        run(tasks, executor, null);
    }

    public static void run(
        final Collection<? extends Runnable> tasks,
        final ExecutorService executor,
        final Collection<Future<?>> futures
    ) {
        awaitTermination(run(tasks, true, executor, futures));
    }

    public static Collection<Future<?>> run(
        final Collection<? extends Runnable> tasks,
        final boolean allowSynchronousRun,
        final ExecutorService executor,
        @Nullable Collection<Future<?>> futures
    ) {

        boolean noExecutor = !canRunInParallel(executor);

        if (allowSynchronousRun && (tasks.size() == 1 || noExecutor)) {
            tasks.forEach(Runnable::run);
            return Collections.emptyList();
        }

        if (noExecutor) {
            throw new IllegalStateException("No running executor provided and synchronous execution is not allowed");
        }

        if (futures == null) {
            futures = new ArrayList<>(tasks.size());
        } else {
            futures.clear();
        }

        for (Runnable task : tasks) {
            futures.add(executor.submit(task));
        }

        return futures;
    }

    static void runWithConcurrency(RunWithConcurrency params) {
        runWithConcurrency(
            params.concurrency(),
            params.tasks(),
            params.forceUsageOfExecutor(),
            params.waitNanos(),
            params.maxWaitRetries(),
            params.mayInterruptIfRunning(),
            params.terminationFlag(),
            params.executor()
        );
    }

    // only called from an existing Params object, rely on its validation
    private static void runWithConcurrency(
        int concurrency,
        Iterator<? extends Runnable> tasks,
        boolean forceUsageOfExecutor,
        long waitNanos,
        long maxWaitRetries,
        boolean mayInterruptIfRunning,
        TerminationFlag terminationFlag,
        @Nullable ExecutorService executor
    ) {
        // Params validation ensures that `forceUsageOfExecutor==true && canRunInParallel(executor)==false` cannot happen
        if (!canRunInParallel(executor) || (concurrency <= 1 && !forceUsageOfExecutor)) {
            while (tasks.hasNext()) {
                Runnable task = tasks.next();
                terminationFlag.assertRunning();
                task.run();
            }
            return;
        }

        CompletionService completionService =
            new CompletionService(executor, concurrency);

        PushbackIterator<Runnable> ts =
            new PushbackIterator<>(tasks);

        Throwable error = null;
        // generally assumes that tasks.size is notably larger than concurrency
        try {
            //noinspection StatementWithEmptyBody - add first concurrency tasks
            for (int i = concurrency; i-- > 0
                                      && terminationFlag.running()
                                      && completionService.trySubmit(ts); )
                ;

            terminationFlag.assertRunning();

            // submit all remaining tasks
            int tries = 0;
            while (ts.hasNext()) {
                if (completionService.hasTasks()) {
                    try {
                        if (!completionService.awaitOrFail()) {
                            continue;
                        }
                    } catch (ExecutionException e) {
                        error = ExceptionUtil.chain(error, e.getCause());
                    } catch (CancellationException ignore) {
                    }
                }

                terminationFlag.assertRunning();

                if (!completionService.trySubmit(ts) && !completionService.hasTasks()) {
                    if (++tries >= maxWaitRetries) {
                        throw new IllegalThreadStateException(formatWithLocale(
                            "Attempted to submit tasks for %d times with a %d nanosecond delay (%d milliseconds) between each attempt, but ran out of time",
                            tries,
                            waitNanos,
                            TimeUnit.NANOSECONDS.toMillis(waitNanos)
                        ));
                    }
                    LockSupport.parkNanos(waitNanos);
                }
            }

            // wait for all tasks to finish
            while (completionService.hasTasks()) {
                terminationFlag.assertRunning();
                try {
                    completionService.awaitOrFail();
                } catch (ExecutionException e) {
                    error = ExceptionUtil.chain(error, e.getCause());
                } catch (CancellationException ignore) {
                }
            }
        } catch (InterruptedException e) {
            error = error == null ? e : ExceptionUtil.chain(e, error);
            Thread.currentThread().interrupt();
        } finally {
            finishRunWithConcurrency(mayInterruptIfRunning, completionService, error);
        }
    }

    private static void finishRunWithConcurrency(
        boolean mayInterruptIfRunning,
        CompletionService completionService,
        @Nullable Throwable error
    ) {
        // cancel all regardless of done flag because we could have aborted
        // from the termination flag
        completionService.cancelAll(mayInterruptIfRunning);
        if (error != null) {
            ExceptionUtil.throwIfUnchecked(error);
            throw new RuntimeException(error);
        }
    }

    public static void awaitTermination(final Collection<Future<?>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException ee) {
                    final Throwable cause = ee.getCause();
                    if (error != cause) {
                        error = ExceptionUtil.chain(error, cause);
                    }
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = ExceptionUtil.chain(e, error);
            Thread.currentThread().interrupt();
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(false);
                }
            }
        }
        if (error != null) {
            ExceptionUtil.throwIfUnchecked(error);
            throw new RuntimeException(error);
        }
    }

    /**
     * Copied from {@link java.util.concurrent.ExecutorCompletionService}
     * and adapted to reduce indirection.
     * Does not support {@link java.util.concurrent.ForkJoinPool} as backing executor.
     */
    private static final class CompletionService {
        private static final int AWAIT_TIMEOUT_MILLIS = 100;

        private final Executor executor;
        private final ThreadPoolExecutor pool;
        private final int availableConcurrency;
        private final Set<Future<Void>> running;
        private final BlockingQueue<Future<Void>> completionQueue;

        private class QueueingFuture extends FutureTask<Void> {
            QueueingFuture(final Runnable runnable) {
                super(runnable, null);
                running.add(this);
            }

            @Override
            protected void done() {
                if (!isCancelled()) {
                    //noinspection StatementWithEmptyBody - spin-wait on free slot
                    while (!completionQueue.offer(this)) ;
                }
                running.remove(this);
            }
        }

        CompletionService(final ExecutorService executor, final int targetConcurrency) {
            if (!canRunInParallel(executor)) {
                throw new IllegalArgumentException(
                    "executor already terminated or not usable");
            }
            if (executor instanceof ThreadPoolExecutor) {
                pool = (ThreadPoolExecutor) executor;
                availableConcurrency = pool.getCorePoolSize();
                int capacity = Math.max(targetConcurrency, availableConcurrency) + 1;
                completionQueue = new ArrayBlockingQueue<>(capacity);
            } else {
                pool = null;
                availableConcurrency = Integer.MAX_VALUE;
                completionQueue = new LinkedBlockingQueue<>();
            }

            this.executor = executor;
            this.running = Collections.newSetFromMap(new ConcurrentHashMap<>());
        }

        boolean trySubmit(final PushbackIterator<Runnable> tasks) {
            if (tasks.hasNext()) {
                Runnable next = tasks.next();
                if (submit(next)) {
                    return true;
                }
                tasks.pushBack(next);
            }
            return false;
        }

        boolean submit(final Runnable task) {
            Objects.requireNonNull(task);
            if (canSubmit()) {
                executor.execute(new QueueingFuture(task));
                return true;
            }
            return false;
        }

        boolean hasTasks() {
            return !(running.isEmpty() && completionQueue.isEmpty());
        }

        boolean awaitOrFail() throws InterruptedException, ExecutionException {
            var task = completionQueue.poll(AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (task == null) {
                return false;
            } else {
                task.get();
                return true;
            }
        }

        void cancelAll(boolean mayInterruptIfRunning) {
            stopFuturesAndStopScheduling(running, mayInterruptIfRunning);
            stopFutures(completionQueue, mayInterruptIfRunning);
        }

        private boolean canSubmit() {
            return pool == null || pool.getActiveCount() < availableConcurrency;
        }

        private void stopFutures(final Collection<Future<Void>> futures, boolean mayInterruptIfRunning) {
            for (Future<Void> future : futures) {
                future.cancel(mayInterruptIfRunning);
            }
            futures.clear();
        }

        private void stopFuturesAndStopScheduling(
            final Collection<Future<Void>> futures,
            boolean mayInterruptIfRunning
        ) {
            if (pool == null) {
                stopFutures(futures, mayInterruptIfRunning);
                return;
            }
            for (Future<Void> future : futures) {
                if (future instanceof Runnable) {
                    pool.remove((Runnable) future);
                }
                future.cancel(mayInterruptIfRunning);
            }
            futures.clear();
            pool.purge();
        }
    }

    private static final class PushbackIterator<T> implements Iterator<T> {
        private final Iterator<? extends T> delegate;
        private T pushedElement;

        private PushbackIterator(final Iterator<? extends T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return pushedElement != null || delegate.hasNext();
        }

        @Override
        public T next() {
            T el;
            if ((el = pushedElement) != null) {
                pushedElement = null;
            } else {
                el = delegate.next();
            }
            return el;
        }

        void pushBack(final T element) {
            if (pushedElement != null) {
                throw new IllegalArgumentException("Cannot push back twice");
            }
            pushedElement = element;
        }
    }
}
