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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Parameters for {@link org.neo4j.gds.core.concurrency.ParallelUtil#runWithConcurrency(RunWithConcurrency)}.
 * <p>
 * Create an instance by using the {@link #builder() Builder}.
 */
@ValueClass
public interface RunWithConcurrency {

    /**
     * Default value for how often a task is retried before giving up.
     * The default is so that retrying {@link #waitTime() every}  {@link #waitTimeUnit() microsecond} will stop after about 3 days.
     *
     * @see #maxWaitRetries()
     */
    long DEFAULT_MAX_NUMBER_OF_RETRIES = (long) 2.5e11; // about 3 days in micros

    /**
     * The maximum concurrency for running the tasks.
     * <p>
     * If the concurrency is 1, the tasks are run sequentially on the calling thread
     * until all tasks are finished or the first Exception is thrown.
     * This behavior can be overridden by setting {@link #forceUsageOfExecutor()} to {@code true}.
     */
    int concurrency();

    /**
     * The tasks that will be executed.
     * <p>
     * The tasks are submitted to the {@link #executor()} only if they can be immediately
     * scheduled and executed. The iterator is only iterated to at most one item in advance.
     * If the task iterator creates the tasks lazily upon iteration, we can avoid creating all tasks upfront.
     * We can support thousands, even millions of tasks without resource exhaustion that way.
     * <p>
     * We will try to submit tasks as long as no more than {@link #concurrency()} are already started
     * and then continue to submit tasks one-by-one, after a previous tasks has finished,
     * so that no more than {@code concurrency} tasks are running in the provided {@code executor}.
     * <p>
     * We will try to submit tasks as long as the {@code executor} can directly start new tasks, that is,
     * we want to avoid creating tasks and put them into the waiting queue if it may never be executed afterwards.
     */
    Iterator<? extends Runnable> tasks();

    /**
     * This setting is only relevant if the {@link #concurrency()} is 1.
     * <p>
     * If {@code forceUsageOfExecutor} is {@code true}, we will submit all tasks to the executor.
     * If {@code forceUsageOfExecutor} is {@code false}, we will run all tasks on the calling thread.
     * <p>
     * Running tasks on the calling thread will exit early once an exception is thrown from a task.
     * Running tasks on the executor will collect all exceptions and run all non failing tasks.
     * <p>
     * The default value is {@code false} and will let tasks run on the calling thread.
     */
    @Value.Default
    default boolean forceUsageOfExecutor() {
        return false;
    }

    /**
     * If the {@link #executor()} is not able to accept any more tasks, we will wait and retry submitting the task.
     * The time to wait is set by {@code waitTime} together with {@link #waitTimeUnit()}.
     * The number of retries is set by {@link #maxWaitRetries()}.
     * <p>
     * After {@code maxWaitRetries} have been exhausted, execution of all further tasks is abandoned and
     * a {@link java.lang.IllegalThreadStateException} is thrown.
     * <p>
     * The default is to retry every 1 microsecond for about 3 days.
     */
    @Value.Default
    default long waitTime() {
        return 1;
    }

    /**
     * If the {@link #executor()} is not able to accept any more tasks, we will wait and retry submitting the task.
     * The time to wait is set by {@link #waitTime()} together with {@code waitTimeUnit}.
     * The number of retries is set by {@link #maxWaitRetries()}.
     * <p>
     * After {@code maxWaitRetries} have been exhausted, execution of all further tasks is abandoned and
     * a {@link java.lang.IllegalThreadStateException} is thrown.
     * <p>
     * The default is to retry every 1 microsecond for about 3 days.
     */
    @Value.Default
    default TimeUnit waitTimeUnit() {
        return TimeUnit.MICROSECONDS;
    }

    /**
     * The actual wait time in nanoseconds.
     *
     * @see #waitTime()
     * @see #waitTimeUnit()
     */
    @Value.Derived
    default long waitNanos() {
        return waitTimeUnit().toNanos(waitTime());
    }

    /**
     * If the {@link #executor()} is not able to accept any more tasks, we will wait and retry submitting the task.
     * The time to wait is set by {@code waitTime} together with {@link  #waitTimeUnit()}.
     * The number of retries is set by {@code maxWaitRetries}.
     * <p>
     * After {@code maxWaitRetries} have been exhausted, execution of all further tasks is abandoned and
     * a {@link java.lang.IllegalThreadStateException} is thrown.
     * <p>
     * The default is to retry every 1 microsecond for about 3 days.
     */
    @Value.Default
    default long maxWaitRetries() {
        return DEFAULT_MAX_NUMBER_OF_RETRIES;
    }

    /**
     * Provide a {@link org.neo4j.gds.termination.TerminationFlag} to support graceful early termination.
     * <p>
     * After the initial number of {@link #concurrency()} tasks have been submitted,
     * the termination flag is checked before submitting any further tasks.
     * If the termination flag triggers, running tasks are asked to stop and outstanding tasks will be abandoned.
     * <p>
     * It is up to the task implementation to also respect the termination flag.
     * <p>
     * The default flag will never trigger.
     */
    @Value.Default
    default TerminationFlag terminationFlag() {
        return TerminationFlag.RUNNING_TRUE;
    }

    /**
     * If the {@link #terminationFlag()} triggers or the calling thread is {@link Thread#interrupt() interrupted},
     * all running tasks are cancelled.
     * This flag is passed to {@link java.util.concurrent.Future#cancel(boolean)}.
     * <p>
     * If {@code mayInterruptIfRunning} is {@code true}, running tasks are interrupted and may exit early if the
     * {@link java.lang.InterruptedException} is handled properly.
     * If {@code mayInterruptIfRunning} is {@code false}, running tasks are allowed to finish on their own.
     * <p>
     * In either case, tasks that have not been started will never start.
     * <p>
     * The default behavior is to interrupt running tasks ({@code true}).
     */
    @Value.Default
    default boolean mayInterruptIfRunning() {
        return true;
    }

    /**
     * The executor that will run the tasks.
     * <p>
     * If the executor is terminated, tasks are run on the calling thread,
     * even for {@link #concurrency() concurrencies} greater than 1.
     * <p>
     * If {@link #forceUsageOfExecutor()} is {@code true} however, an {@link java.lang.IllegalArgumentException}
     * is thrown when this object is constructed.
     * <p>
     * The default executor is {@link DefaultPool#INSTANCE}.
     */
    @Value.Default
    default @Nullable ExecutorService executor() {
        return DefaultPool.INSTANCE;
    }

    /**
     * Try to run all tasks for their side effects using at most {@link #concurrency()} threads at once.
     */
    default void run() {
        ParallelUtil.runWithConcurrency(this);
    }

    /**
     * Returns a new {@link org.neo4j.gds.core.concurrency.RunWithConcurrency.Builder}.
     */
    static Builder builder() {
        return new Builder();
    }

    final class Builder extends ImmutableRunWithConcurrency.Builder {

        /**
         * Provides the tasks from an existing {@code Iterable}.
         */
        public Builder tasks(Iterable<? extends Runnable> tasks) {
            return this.tasks(tasks.iterator());
        }

        /**
         * Provides the tasks from a {@code Stream} of tasks by using {@link java.util.stream.Stream#iterator()}.
         */
        public Builder tasks(Stream<? extends Runnable> tasks) {
            return this.tasks(tasks.iterator());
        }

        /**
         * Sets the {@link RunWithConcurrency#waitTime()} together with its {@link RunWithConcurrency#waitTimeUnit()}
         * Provides the tasks from an existing {@code Iterable}.
         */
        public Builder waitTime(long waitTime, TimeUnit waitTimeUnit) {
            return this.waitTime(waitTime).waitTimeUnit(waitTimeUnit);
        }

        /**
         * Try to run all tasks for their side effects using at most {@link #concurrency()} threads at once.
         * <p>
         * This will build the {@link org.neo4j.gds.core.concurrency.RunWithConcurrency} object and immediately call {@link RunWithConcurrency#run()}.
         */
        public void run() {
            this.build().run();
        }
    }

    @Value.Check
    default void validate() {
        if (concurrency() < 0) {
            throw new IllegalArgumentException("[concurrency] must be at least 0, but got " + concurrency());
        }
        if (waitTime() < 0) {
            throw new IllegalArgumentException("[waitTime] must be at least 0, but got " + waitTime());
        }
        if (forceUsageOfExecutor() && !ParallelUtil.canRunInParallel(executor())) {
            throw new IllegalArgumentException(
                "[executor] cannot be used to run tasks because is terminated or shut down.");
        }
    }
}
