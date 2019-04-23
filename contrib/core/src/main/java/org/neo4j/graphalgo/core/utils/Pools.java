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

import org.neo4j.helpers.NamedThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class Pools {

    private static final int MAX_CONCURRENCY;
    public static final int DEFAULT_CONCURRENCY;

    static {
        ConcurrencyConfig concurrencyConfig = ConcurrencyConfig.of();
        MAX_CONCURRENCY = concurrencyConfig.maxConcurrency;
        DEFAULT_CONCURRENCY = concurrencyConfig.defaultConcurrency;
    }

    public static int allowedConcurrency(int concurrency) {
        return Math.min(MAX_CONCURRENCY, concurrency);
    }

    public static final int DEFAULT_QUEUE_SIZE = DEFAULT_CONCURRENCY * 50;

    public static final ExecutorService DEFAULT = createDefaultPool();
    public static final ForkJoinPool FJ_POOL = createFJPool();

    private Pools() {
        throw new UnsupportedOperationException();
    }

    public static ExecutorService createDefaultPool() {
        return new ThreadPoolExecutor(
                DEFAULT_CONCURRENCY,
                DEFAULT_CONCURRENCY * 2,
                30L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(DEFAULT_QUEUE_SIZE),
                NamedThreadFactory.daemon("algo"),
                new CallerBlocksPolicy());
    }

    public static ForkJoinPool createFJPool() {
        return new ForkJoinPool(ForkJoinPool.getCommonPoolParallelism());
    }

    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                // block caller for 100ns
                LockSupport.parkNanos(100);
                try {
                    // submit again
                    executor.submit(r).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
