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

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PoolsTest {

    @Test
    void shouldGetLimitedPool() {
        ConcurrencyMonitor.instance().setLimited();
        ThreadPoolExecutor defaultPool = (ThreadPoolExecutor) Pools.createDefaultPool();

        assertEquals(4, defaultPool.getCorePoolSize());
        assertEquals(4, defaultPool.getMaximumPoolSize());
    }

    @Test
    void shouldGetUnlimitedPool() {
        ConcurrencyMonitor.instance().setUnlimited();
        ThreadPoolExecutor defaultPool = (ThreadPoolExecutor) Pools.createDefaultPool();

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        assertEquals(availableProcessors, defaultPool.getCorePoolSize());
        assertEquals(2 * availableProcessors, defaultPool.getMaximumPoolSize());
    }


}