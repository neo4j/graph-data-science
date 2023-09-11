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

import org.neo4j.gds.concurrency.PoolSizes;
import org.neo4j.gds.concurrency.PoolSizesService;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ExecutorServices {

    public static final ExecutorService DEFAULT = createDefaultPool(PoolSizesService.poolSizes());

    private static ExecutorService createDefaultPool(PoolSizes poolSizes) {
        return new ThreadPoolExecutor(
            poolSizes.corePoolSize(),
            poolSizes.maxPoolSize(),
            30L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(poolSizes.corePoolSize() * 50),
            Pools.DEFAULT_THREAD_FACTORY,
            new Pools.CallerBlocksPolicy()
        );
    }

    private ExecutorServices() {}
}
