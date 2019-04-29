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

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.HugeBatchNodeIterable;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.helpers.Exceptions;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

public final class ParallelUtil {

    public static final int DEFAULT_BATCH_SIZE = 10_000;


    public static Collection<PrimitiveIntIterable> batchIterables(int concurrency, int nodeCount) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be > 0");
        }
        final int batchSize = Math.max(1, nodeCount / concurrency);
        int numberOfBatches = ParallelUtil.threadSize(batchSize, nodeCount);
        if (numberOfBatches == 1) {
            return Collections.singleton(new IdMap.IdIterable(0, nodeCount));
        }
        PrimitiveIntIterable[] iterators = new PrimitiveIntIterable[numberOfBatches];
        Arrays.setAll(iterators, i -> {
            int start = i * batchSize;
            int length = Math.min(batchSize, nodeCount - start);
            return new IdMap.IdIterable(start, length);
        });
        return Arrays.asList(iterators);
    }

    public static int threadSize(int batchSize, int elementCount) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
        if (batchSize >= elementCount) {
            return 1;
        }
        return (int) Math.ceil(elementCount / (double) batchSize);
    }

    public static long threadSize(int batchSize, long elementCount) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
        if (batchSize >= elementCount) {
            return 1;
        }
        return (long) Math.ceil(elementCount / (double) batchSize);
    }

    public static long threadSize(long batchSize, long elementCount) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
        if (batchSize >= elementCount) {
            return 1;
        }
        return (long) Math.ceil(elementCount / (double) batchSize);
    }

    public static int adjustBatchSize(
            int nodeCount,
            int concurrency,
            int minBatchSize) {
        if (concurrency <= 0) {
            concurrency = nodeCount;
        }
        int targetBatchSize = threadSize(concurrency, nodeCount);
        return Math.max(minBatchSize, targetBatchSize);
    }

    public static int adjustBatchSize(
            int nodeCount,
            int concurrency) {
        return adjustBatchSize(nodeCount, concurrency, DEFAULT_BATCH_SIZE);
    }


    public static long adjustBatchSize(
            long nodeCount,
            int concurrency,
            long minBatchSize) {
        if (concurrency <= 0) {
            concurrency = (int) Math.min(nodeCount, (long) Integer.MAX_VALUE);
        }
        long targetBatchSize = threadSize(concurrency, nodeCount);
        return Math.max(minBatchSize, targetBatchSize);
    }

    public static long adjustBatchSize(
            long nodeCount,
            int concurrency,
            long minBatchSize,
            long maxBatchSize) {
        return Math.min(maxBatchSize, adjustBatchSize(nodeCount, concurrency, minBatchSize));
    }

    public static boolean canRunInParallel(ExecutorService executor) {
        return executor != null && !(executor.isShutdown() || executor.isTerminated());
    }

    public static int availableThreads(ExecutorService executor) {
        if (!canRunInParallel(executor)) {
            return 0;
        }
        if (executor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor pool = (ThreadPoolExecutor) executor;
            // TPE only increases to max threads if the queue is full, (see their JavaDoc)
            // so the number of threads available that are guaranteed to start immediately is
            // only based on the number of core threads.
            return pool.getCorePoolSize() - pool.getActiveCount();
        }
        // If we have another pool, we just have to hope for the best (or, maybe throw?)
        return Integer.MAX_VALUE;
    }

    /**
     * Executes read operations in parallel, based on the given batch size
     * and executor.
     */
    public static <T extends Runnable> List<T> readParallel(
            int concurrency,
            int batchSize,
            BatchNodeIterable idMapping,
            ParallelGraphImporter<T> importer,
            ExecutorService executor) {

        Collection<PrimitiveIntIterable> iterators =
                idMapping.batchIterables(batchSize);

        int threads = iterators.size();

        if (!canRunInParallel(executor) || threads == 1) {
            int nodeOffset = 0;
            List<T> tasks = new ArrayList<>(threads);
            for (PrimitiveIntIterable iterator : iterators) {
                final T task = importer.newImporter(nodeOffset, iterator);
                tasks.add(task);
                task.run();
                nodeOffset += batchSize;
            }
            return tasks;
        } else {
            List<T> tasks = new ArrayList<>(threads);
            int nodeOffset = 0;
            for (PrimitiveIntIterable iterator : iterators) {
                tasks.add(importer.newImporter(nodeOffset, iterator));
                nodeOffset += batchSize;
            }
            runWithConcurrency(concurrency, tasks, executor);
            return tasks;
        }
    }

    /**
     * Executes read operations in parallel, based on the given batch size
     * and executor.
     */
    public static <T extends Runnable> List<T> readParallel(
            int concurrency,
            int batchSize,
            HugeBatchNodeIterable idMapping,
            ExecutorService executor,
            HugeParallelGraphImporter<T> importer) {

        Collection<PrimitiveLongIterable> iterators =
                idMapping.hugeBatchIterables(batchSize);

        int threads = iterators.size();

        final List<T> tasks = new ArrayList<>(threads);
        if (!canRunInParallel(executor) || threads == 1) {
            long nodeOffset = 0L;
            for (PrimitiveLongIterable iterator : iterators) {
                final T task = importer.newImporter(nodeOffset, iterator);
                tasks.add(task);
                task.run();
                nodeOffset += batchSize;
            }
        } else {
            AtomicLong nodeOffset = new AtomicLong();
            Collection<T> importers = LazyMappingCollection.of(
                    iterators,
                    it -> {
                        T task = importer.newImporter(nodeOffset.getAndAdd(batchSize), it);
                        tasks.add(task);
                        return task;
                    });
            runWithConcurrency(concurrency, importers, executor);
        }
        return tasks;
    }

    public static Collection<Runnable> tasks(
            final int concurrency,
            final Supplier<? extends Runnable> newTask) {
        final List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(newTask.get());
        }
        return tasks;
    }

    /**
     * Runs a collection of {@link Runnable}s in parallel for their side-effects.
     * The level of parallelism is defined by the given executor.
     * <p>
     * This is similar to {@link ExecutorService#invokeAll(Collection)},
     * except that all Exceptions thrown by any task are chained together.
     */
    public static void run(
            Collection<? extends Runnable> tasks,
            ExecutorService executor) {
        run(tasks, executor, null);
    }

    public static void run(
            Collection<? extends Runnable> tasks,
            ExecutorService executor,
            Collection<Future<?>> futures) {
        awaitTermination(run(tasks, true, executor, futures));
    }

    public static Collection<Future<?>> run(
            Collection<? extends Runnable> tasks,
            boolean allowSynchronousRun,
            ExecutorService executor,
            Collection<Future<?>> futures) {

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

    public static void run(
            Collection<? extends Runnable> tasks,
            Runnable selfTask,
            ExecutorService executor,
            Collection<Future<?>> futures) {

        if (tasks.size() == 0) {
            selfTask.run();
            return;
        }

        if (null == executor) {
            tasks.forEach(Runnable::run);
            selfTask.run();
            return;
        }

        if (executor.isShutdown() || executor.isTerminated()) {
            throw new IllegalStateException("Executor is shut down");
        }

        if (futures == null) {
            futures = new ArrayList<>(tasks.size());
        } else {
            futures.clear();
        }

        for (Runnable task : tasks) {
            futures.add(executor.submit(task));
        }

        awaitTermination(futures);
    }

    /**
     * Try to run all tasks for their side-effects using at most
     * {@code concurrency} threads at once.
     * <p>
     * If the concurrency is 1 or less, or there is only a single task, or the
     * provided {@link ExecutorService} has terminated the tasks are run
     * sequentially on the calling thread until all tasks are finished or the
     * first Exception is thrown.
     * <p>
     * If the tasks are submitted to the {@code executor}, it may happen that
     * not all tasks are actually executed. If the provided collection creates
     * the tasks lazily upon iteration, not all elements might actually be
     * created.
     * <p>
     * The calling thread will be always blocked during the execution of the tasks
     * and is not available for scheduling purposes. If the calling thread is
     * {@link Thread#interrupt() interrupted} during the execution, running tasks
     * are cancelled and not-yet-started tasks are abandoned.
     * <p>
     * We will try to submit tasks as long as no more than {@code concurrency}
     * are already started and then continue to submit tasks one-by-one, after
     * a previous tasks has finished, so that no more than {@code concurrency}
     * tasks are running in the provided {@code executor}.
     * <p>
     * We do not submit all tasks at-once into the worker queue to avoid creating
     * a large amount of tasks and further support lazily created tasks.
     * We can support thousands, even millions of tasks without resource exhaustion that way.
     * <p>
     * We will try to submit tasks as long as the {@code executor} can
     * directly start new tasks, that is, we want to avoid creating tasks and put
     * them into the waiting queue if it may never be executed afterwards.
     * <p>
     * If the pool is full, all remaining, non-submitted tasks are abandoned
     * and never tried again.
     *
     * @param concurrency how many tasks should be run simultaneous
     * @param tasks       the tasks to execute
     * @param executor    the executor to submit the tasks to
     */
    public static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            ExecutorService executor) {
        runWithConcurrency(
                concurrency,
                tasks,
                0,
                0,
                TerminationFlag.RUNNING_TRUE,
                executor);
    }

    /**
     * Try to run all tasks for their side-effects using at most
     * {@code concurrency} threads at once.
     * <p>
     * If the concurrency is 1 or less, or there is only a single task, or the
     * provided {@link ExecutorService} has terminated the tasks are run
     * sequentially on the calling thread until all tasks are finished or the
     * first Exception is thrown.
     * <p>
     * If the tasks are submitted to the {@code executor}, it may happen that
     * not all tasks are actually executed. If the provided collection creates
     * the tasks lazily upon iteration, not all elements might actually be
     * created.
     * <p>
     * The calling thread will be always blocked during the execution of the tasks
     * and is not available for scheduling purposes. If the calling thread is
     * {@link Thread#interrupt() interrupted} during the execution, running tasks
     * are cancelled and not-yet-started tasks are abandoned.
     * <p>
     * We will try to submit tasks as long as no more than {@code concurrency}
     * are already started and then continue to submit tasks one-by-one, after
     * a previous tasks has finished, so that no more than {@code concurrency}
     * tasks are running in the provided {@code executor}.
     * <p>
     * We do not submit all tasks at-once into the worker queue to avoid creating
     * a large amount of tasks and further support lazily created tasks.
     * We can support thousands, even millions of tasks without resource exhaustion that way.
     * <p>
     * We will try to submit tasks as long as the {@code executor} can
     * directly start new tasks, that is, we want to avoid creating tasks and put
     * them into the waiting queue if it may never be executed afterwards.
     * <p>
     * If the pool is full, all remaining, non-submitted tasks are abandoned
     * and never tried again.
     * <p>
     * The provided {@code terminationFlag} is checked before submitting new
     * tasks and if it signals termination, running tasks are cancelled and
     * not-yet-started tasks are abandoned.
     *
     * @param concurrency     how many tasks should be run simultaneous
     * @param tasks           the tasks to execute
     * @param terminationFlag a flag to check periodically if the execution should be terminated
     * @param executor        the executor to submit the tasks to
     */
    public static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            TerminationFlag terminationFlag,
            ExecutorService executor) {
        runWithConcurrency(
                concurrency,
                tasks,
                0,
                0,
                terminationFlag,
                executor);
    }

    /**
     * Try to run all tasks for their side-effects using at most
     * {@code concurrency} threads at once.
     * <p>
     * If the concurrency is 1 or less, or there is only a single task, or the
     * provided {@link ExecutorService} has terminated the tasks are run
     * sequentially on the calling thread until all tasks are finished or the
     * first Exception is thrown.
     * <p>
     * If the tasks are submitted to the {@code executor}, it may happen that
     * not all tasks are actually executed. If the provided collection creates
     * the tasks lazily upon iteration, not all elements might actually be
     * created.
     * <p>
     * The calling thread will be always blocked during the execution of the tasks
     * and is not available for scheduling purposes. If the calling thread is
     * {@link Thread#interrupt() interrupted} during the execution, running tasks
     * are cancelled and not-yet-started tasks are abandoned.
     * <p>
     * We will try to submit tasks as long as no more than {@code concurrency}
     * are already started and then continue to submit tasks one-by-one, after
     * a previous tasks has finished, so that no more than {@code concurrency}
     * tasks are running in the provided {@code executor}.
     * <p>
     * We do not submit all tasks at-once into the worker queue to avoid creating
     * a large amount of tasks and further support lazily created tasks.
     * We can support thousands, even millions of tasks without resource exhaustion that way.
     * <p>
     * We will try to submit tasks as long as the {@code executor} can
     * directly start new tasks, that is, we want to avoid creating tasks and put
     * them into the waiting queue if it may never be executed afterwards.
     * <p>
     * If the pool is full, wait for {@code waitTime} {@code timeUnit}s
     * and retry submitting the tasks indefinitely.
     *
     * @param concurrency how many tasks should be run simultaneous
     * @param tasks       the tasks to execute
     * @param waitTime    how long to wait between retries
     * @param timeUnit    the unit for {@code waitTime}
     * @param executor    the executor to submit the tasks to
     */
    public static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            long waitTime,
            TimeUnit timeUnit,
            ExecutorService executor) {
        runWithConcurrency(
                concurrency,
                tasks,
                timeUnit.toNanos(waitTime),
                Integer.MAX_VALUE,
                TerminationFlag.RUNNING_TRUE,
                executor);
    }

    /**
     * Try to run all tasks for their side-effects using at most
     * {@code concurrency} threads at once.
     * <p>
     * If the concurrency is 1 or less, or there is only a single task, or the
     * provided {@link ExecutorService} has terminated the tasks are run
     * sequentially on the calling thread until all tasks are finished or the
     * first Exception is thrown.
     * <p>
     * If the tasks are submitted to the {@code executor}, it may happen that
     * not all tasks are actually executed. If the provided collection creates
     * the tasks lazily upon iteration, not all elements might actually be
     * created.
     * <p>
     * The calling thread will be always blocked during the execution of the tasks
     * and is not available for scheduling purposes. If the calling thread is
     * {@link Thread#interrupt() interrupted} during the execution, running tasks
     * are cancelled and not-yet-started tasks are abandoned.
     * <p>
     * We will try to submit tasks as long as no more than {@code concurrency}
     * are already started and then continue to submit tasks one-by-one, after
     * a previous tasks has finished, so that no more than {@code concurrency}
     * tasks are running in the provided {@code executor}.
     * <p>
     * We do not submit all tasks at-once into the worker queue to avoid creating
     * a large amount of tasks and further support lazily created tasks.
     * We can support thousands, even millions of tasks without resource exhaustion that way.
     * <p>
     * We will try to submit tasks as long as the {@code executor} can
     * directly start new tasks, that is, we want to avoid creating tasks and put
     * them into the waiting queue if it may never be executed afterwards.
     * <p>
     * If the pool is full, wait for {@code waitTime} {@code timeUnit}s
     * and retry submitting the tasks indefinitely.
     * <p>
     * The provided {@code terminationFlag} is checked before submitting new
     * tasks and if it signals termination, running tasks are cancelled and
     * not-yet-started tasks are abandoned.
     *
     * @param concurrency     how many tasks should be run simultaneous
     * @param tasks           the tasks to execute
     * @param waitTime        how long to wait between retries
     * @param timeUnit        the unit for {@code waitTime}
     * @param terminationFlag a flag to check periodically if the execution should be terminated
     * @param executor        the executor to submit the tasks to
     */
    public static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            long waitTime,
            TimeUnit timeUnit,
            TerminationFlag terminationFlag,
            ExecutorService executor) {
        runWithConcurrency(
                concurrency,
                tasks,
                timeUnit.toNanos(waitTime),
                Integer.MAX_VALUE,
                terminationFlag,
                executor);
    }

    /**
     * Try to run all tasks for their side-effects using at most
     * {@code concurrency} threads at once.
     * <p>
     * If the concurrency is 1 or less, or there is only a single task, or the
     * provided {@link ExecutorService} has terminated the tasks are run
     * sequentially on the calling thread until all tasks are finished or the
     * first Exception is thrown.
     * <p>
     * If the tasks are submitted to the {@code executor}, it may happen that
     * not all tasks are actually executed. If the provided collection creates
     * the tasks lazily upon iteration, not all elements might actually be
     * created.
     * <p>
     * The calling thread will be always blocked during the execution of the tasks
     * and is not available for scheduling purposes. If the calling thread is
     * {@link Thread#interrupt() interrupted} during the execution, running tasks
     * are cancelled and not-yet-started tasks are abandoned.
     * <p>
     * We will try to submit tasks as long as no more than {@code concurrency}
     * are already started and then continue to submit tasks one-by-one, after
     * a previous tasks has finished, so that no more than {@code concurrency}
     * tasks are running in the provided {@code executor}.
     * <p>
     * We do not submit all tasks at-once into the worker queue to avoid creating
     * a large amount of tasks and further support lazily created tasks.
     * We can support thousands, even millions of tasks without resource exhaustion that way.
     * <p>
     * We will try to submit tasks as long as the {@code executor} can
     * directly start new tasks, that is, we want to avoid creating tasks and put
     * them into the waiting queue if it may never be executed afterwards.
     * <p>
     * If the pool is full, wait for {@code waitTime} {@code timeUnit}s
     * and retry submitting the tasks at most {@code maxRetries} times.
     *
     * @param concurrency how many tasks should be run simultaneous
     * @param tasks       the tasks to execute
     * @param maxRetries  how many retries when submitting on a full pool before giving up
     * @param waitTime    how long to wait between retries
     * @param timeUnit    the unit for {@code waitTime}
     * @param executor    the executor to submit the tasks to
     */
    public static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            int maxRetries,
            long waitTime,
            TimeUnit timeUnit,
            ExecutorService executor) {
        runWithConcurrency(
                concurrency,
                tasks,
                timeUnit.toNanos(waitTime),
                maxRetries,
                TerminationFlag.RUNNING_TRUE,
                executor);
    }

    /**
     * Try to run all tasks for their side-effects using at most
     * {@code concurrency} threads at once.
     * <p>
     * If the concurrency is 1 or less, or there is only a single task, or the
     * provided {@link ExecutorService} has terminated the tasks are run
     * sequentially on the calling thread until all tasks are finished or the
     * first Exception is thrown.
     * <p>
     * If the tasks are submitted to the {@code executor}, it may happen that
     * not all tasks are actually executed. If the provided collection creates
     * the tasks lazily upon iteration, not all elements might actually be
     * created.
     * <p>
     * The calling thread will be always blocked during the execution of the tasks
     * and is not available for scheduling purposes. If the calling thread is
     * {@link Thread#interrupt() interrupted} during the execution, running tasks
     * are cancelled and not-yet-started tasks are abandoned.
     * <p>
     * We will try to submit tasks as long as no more than {@code concurrency}
     * are already started and then continue to submit tasks one-by-one, after
     * a previous tasks has finished, so that no more than {@code concurrency}
     * tasks are running in the provided {@code executor}.
     * <p>
     * We do not submit all tasks at-once into the worker queue to avoid creating
     * a large amount of tasks and further support lazily created tasks.
     * We can support thousands, even millions of tasks without resource exhaustion that way.
     * <p>
     * We will try to submit tasks as long as the {@code executor} can
     * directly start new tasks, that is, we want to avoid creating tasks and put
     * them into the waiting queue if it may never be executed afterwards.
     * <p>
     * If the pool is full, wait for {@code waitTime} {@code timeUnit}s
     * and retry submitting the tasks at most {@code maxRetries} times.
     * <p>
     * The provided {@code terminationFlag} is checked before submitting new
     * tasks and if it signals termination, running tasks are cancelled and
     * not-yet-started tasks are abandoned.
     *
     * @param concurrency     how many tasks should be run simultaneous
     * @param tasks           the tasks to execute
     * @param maxRetries      how many retries when submitting on a full pool before giving up
     * @param waitTime        how long to wait between retries
     * @param timeUnit        the unit for {@code waitTime}
     * @param terminationFlag a flag to check periodically if the execution should be terminated
     * @param executor        the executor to submit the tasks to
     */
    public static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            int maxRetries,
            long waitTime,
            TimeUnit timeUnit,
            TerminationFlag terminationFlag,
            ExecutorService executor) {
        runWithConcurrency(
                concurrency,
                tasks,
                timeUnit.toNanos(waitTime),
                maxRetries,
                terminationFlag,
                executor);
    }

    private static void runWithConcurrency(
            int concurrency,
            Collection<? extends Runnable> tasks,
            long waitNanos,
            int maxWaitRetries,
            TerminationFlag terminationFlag,
            ExecutorService executor) {
        if (!canRunInParallel(executor)
                || tasks.size() == 1
                || concurrency <= 1) {
            Iterator<? extends Runnable> iterator = tasks.iterator();
            while (iterator.hasNext() && terminationFlag.running()) {
                iterator.next().run();
            }
            return;
        }

        CompletionService completionService =
                new CompletionService(executor, concurrency);

        PushbackIterator<Runnable> ts =
                new PushbackIterator<>(tasks.iterator());

        Throwable error = null;
        // generally assumes that tasks.size is notably larger than concurrency
        try {
            //noinspection StatementWithEmptyBody - add first concurrency tasks
            while (concurrency-- > 0
                    && terminationFlag.running()
                    && completionService.trySubmit(ts)) ;

            if (!terminationFlag.running()) {
                return;
            }

            // submit all remaining tasks
            int tries = 0;
            while (ts.hasNext()) {
                if (completionService.hasTasks()) {
                    try {
                        completionService.awaitNext();
                    } catch (ExecutionException e) {
                        error = Exceptions.chain(error, e.getCause());
                    } catch (CancellationException ignore) {
                    }
                }
                if (!terminationFlag.running()) {
                    return;
                }
                if (!completionService.trySubmit(ts) && !completionService.hasTasks()) {
                    if (++tries >= maxWaitRetries) {
                        break;
                    }
                    LockSupport.parkNanos(waitNanos);
                }
            }

            // wait for all tasks to finish
            while (completionService.hasTasks() && terminationFlag.running()) {
                try {
                    completionService.awaitNext();
                } catch (ExecutionException e) {
                    error = Exceptions.chain(error, e.getCause());
                } catch (CancellationException ignore) {
                }
            }
        } catch (InterruptedException e) {
            error = error == null ? e : Exceptions.chain(e, error);
        } finally {
            finishRunWithConcurrency(completionService, error);
        }
    }

    private static void finishRunWithConcurrency(
            CompletionService completionService,
            Throwable error) {
        // cancel all regardless of done flag because we could have aborted
        // from the termination flag
        completionService.cancelAll();
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
    }

    public static void awaitTermination(Collection<Future<?>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException ee) {
                    final Throwable cause = ee.getCause();
                    if (error != cause) {
                        error = Exceptions.chain(error, cause);
                    }
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
    }

    public static void awaitTerminations(Queue<Future<?>> futures) {
        boolean done = false;
        Throwable error = null;
        try {
            while (!futures.isEmpty()) {
                try {
                    futures.poll().get();
                } catch (ExecutionException ee) {
                    error = Exceptions.chain(error, ee.getCause());
                } catch (CancellationException ignore) {
                }
            }
            done = true;
        } catch (InterruptedException e) {
            error = Exceptions.chain(e, error);
        } finally {
            if (!done) {
                for (final Future<?> future : futures) {
                    future.cancel(true);
                }
            }
        }
        if (error != null) {
            throw Exceptions.launderedException(error);
        }
    }

    public static void iterateParallel(
            ExecutorService executorService,
            int size,
            int concurrency,
            IntConsumer consumer) {
        final List<Future<?>> futures = new ArrayList<>();
        final int batchSize = threadSize(concurrency, size);
        for (int i = 0; i < size; i += batchSize) {
            final int start = i;
            final int end = Math.min(size, start + batchSize);
            futures.add(executorService.submit(() -> {
                for (int j = start; j < end; j++) {
                    consumer.accept(j);
                }
            }));
        }
        awaitTermination(futures);
    }


    public static void iterateParallelHuge(
            ExecutorService executorService,
            long size,
            int concurrency,
            LongConsumer consumer) {
        final List<Future<?>> futures = new ArrayList<>();
        final long batchSize = threadSize(concurrency, size);
        for (long i = 0; i < size; i += batchSize) {
            final long start = i;
            final long end = Math.min(size, start + batchSize);
            futures.add(executorService.submit(() -> {
                for (long j = start; j < end; j++) {
                    consumer.accept(j);
                }
            }));
        }
        awaitTermination(futures);
    }


    /**
     * Copied from {@link java.util.concurrent.ExecutorCompletionService}
     * and adapted to reduce indirection.
     * Does not support {@link java.util.concurrent.ForkJoinPool} as backing executor.
     */
    private static final class CompletionService {
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
                running.remove(this);
                if (!isCancelled()) {
                    //noinspection StatementWithEmptyBody - spin-wait on free slot
                    while (!completionQueue.offer(this)) ;
                }
            }
        }

        CompletionService(ExecutorService executor, int targetConcurrency) {
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

        boolean trySubmit(PushbackIterator<Runnable> tasks) {
            if (tasks.hasNext()) {
                Runnable next = tasks.next();
                if (submit(next)) {
                    return true;
                }
                tasks.pushBack(next);
            }
            return false;
        }

        boolean submit(Runnable task) {
            Objects.requireNonNull(task);
            if (canSubmit()) {
                QueueingFuture future = new QueueingFuture(task);
                executor.execute(future);
                return true;
            }
            return false;
        }

        boolean hasTasks() {
            return !(running.isEmpty() && completionQueue.isEmpty());
        }

        void awaitNext() throws InterruptedException, ExecutionException {
            completionQueue.take().get();
        }

        void cancelAll() {
            stopFuturesAndStopScheduling(running);
            stopFutures(completionQueue);
        }

        private boolean canSubmit() {
            return pool == null || pool.getActiveCount() < availableConcurrency;
        }

        private void stopFutures(Collection<Future<Void>> futures) {
            for (Future<Void> future : futures) {
                future.cancel(true);
            }
            futures.clear();
        }

        private void stopFuturesAndStopScheduling(Collection<Future<Void>> futures) {
            if (pool == null) {
                stopFutures(futures);
                return;
            }
            for (Future<Void> future : futures) {
                if (future instanceof Runnable) {
                    pool.remove((Runnable) future);
                }
                future.cancel(true);
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

        void pushBack(T element) {
            if (pushedElement != null) {
                throw new IllegalArgumentException("Cannot push back twice");
            }
            pushedElement = element;
        }
    }
}
