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
package org.neo4j.graphalgo.core.concurrency;

import org.neo4j.graphalgo.compat.NamedThreadFactoryProxy;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.neo4j.graphalgo.config.ConcurrencyValidation.CONCURRENCY_LIMITATION;

public final class Pools {

    public static final ExecutorService DEFAULT = createDefaultPool();
    public static final ExecutorService DEFAULT_SINGLE_THREAD_POOL = createDefaultSingleThreadPool();

    private Pools() {
        throw new UnsupportedOperationException();
    }

    static ExecutorService createDefaultPool() {
        int corePoolSize, maxPoolSize;
        if (ConcurrencyMonitor.instance().isUnlimited()) {
            corePoolSize = Runtime.getRuntime().availableProcessors();
            maxPoolSize = corePoolSize * 2;
        } else {
            corePoolSize = maxPoolSize = CONCURRENCY_LIMITATION;
        }

        return new ThreadPoolExecutor(
            corePoolSize,
            maxPoolSize,
            30L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(corePoolSize * 50),
            NamedThreadFactoryProxy.daemon(),
            new CallerBlocksPolicy()
        );
    }

    public static ExecutorService createDefaultSingleThreadPool() {
        return Executors.newSingleThreadExecutor(NamedThreadFactoryProxy.daemon());
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
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
