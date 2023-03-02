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

import org.neo4j.gds.concurrency.PoolSizesService;
import org.neo4j.internal.helpers.NamedThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class Pools {

    private static final String THREAD_NAME_PREFIX = "gds";

    public static final ThreadFactory DEFAULT_THREAD_FACTORY = NamedThreadFactory.daemon(THREAD_NAME_PREFIX);
    public static final ExecutorService DEFAULT = createDefaultPool();
    public static final ExecutorService DEFAULT_SINGLE_THREAD_POOL = createSingleThreadPool("algo");

    private Pools() {
        throw new UnsupportedOperationException();
    }

    public static ExecutorService createDefaultPool() {
        var poolSizes = PoolSizesService.poolSizes();
        return new ThreadPoolExecutor(
            poolSizes.corePoolSize(),
            poolSizes.maxPoolSize(),
            30L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(poolSizes.corePoolSize() * 50),
            DEFAULT_THREAD_FACTORY,
            new CallerBlocksPolicy()
        );
    }

    public static ExecutorService createSingleThreadPool(String threadPrefix) {
        return Executors.newSingleThreadExecutor(NamedThreadFactory.daemon(threadPrefix));
    }

    public static ForkJoinPool createForkJoinPool(int concurrency) {
        return new ForkJoinPool(concurrency, FJ_WORKER_THREAD_FACTORY, null, false);
    }

    public static Thread newThread(Runnable code) {
        return DEFAULT_THREAD_FACTORY.newThread(code);
    }

    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            // Submit again by directly injecting the task into the work queue, waiting if necessary, but also
            // periodically checking if the pool has been shut down.
            FutureTask<Void> task = new FutureTask<>(r, null);
            BlockingQueue<Runnable> queue = executor.getQueue();
            while (!executor.isShutdown()) {
                try {
                    if (queue.offer(task, 250, TimeUnit.MILLISECONDS)) {
                        while (!executor.isShutdown()) {
                            try {
                                task.get(250, TimeUnit.MILLISECONDS);
                                return; // Success!
                            } catch (TimeoutException ignore) {
                                // This is fine and expected. We just want to check that the executor hasn't been shut down.
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static final ForkJoinPool.ForkJoinWorkerThreadFactory FJ_WORKER_THREAD_FACTORY = pool -> {
        var worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName(Pools.THREAD_NAME_PREFIX + "-forkjoin-" + worker.getPoolIndex());
        return worker;
    };
}
