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
package org.neo4j.graphalgo.core.utils;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.core.concurrency.ConcurrencyMonitor;

import java.util.concurrent.ForkJoinPool;

import static org.neo4j.graphalgo.config.ConcurrencyValidation.CONCURRENCY_LIMITATION;

public final class ForkJoinPools {

    private static ForkJoinPools INSTANCE;

    public static ForkJoinPools instance() {
        if (INSTANCE == null) {
            INSTANCE = new ForkJoinPools();
        }
        return INSTANCE;
    }

    private final ForkJoinPool pool;

    private ForkJoinPools() {
        if (ConcurrencyMonitor.instance().isUnlimited()) {
            pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
        } else {
            pool = new ForkJoinPool(CONCURRENCY_LIMITATION);
        }
    }

    ForkJoinPool getPool() {
        return pool;
    }

    @TestOnly
    static void reset() {
        INSTANCE = null;
    }
}
