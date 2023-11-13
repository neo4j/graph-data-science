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

import org.agrona.concurrent.BackoffIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncBarrier {

    private final AtomicInteger workerCount;
    private final AtomicBoolean isSyncing;
    private final BackoffIdleStrategy idleStrategy;
    private final Runnable rejectAction;

    public static SyncBarrier create() {
        return new SyncBarrier(() -> {});
    }

    public static SyncBarrier create(Runnable rejectAction) {
        return new SyncBarrier(rejectAction);
    }

    private SyncBarrier(Runnable rejectAction) {
        this.workerCount = new AtomicInteger(0);
        this.isSyncing = new AtomicBoolean(false);
        this.idleStrategy = new BackoffIdleStrategy();
        this.rejectAction = rejectAction;
    }

    public void startWorker() {
        if (isSyncing.get()) {
            this.rejectAction.run();
        }
        workerCount.incrementAndGet();
    }

    public void stopWorker() {
        workerCount.decrementAndGet();
    }

    public void sync() {
        this.isSyncing.set(true);
        // wait for import processes to finish
        while (workerCount.get() > 0) {
            idleStrategy.idle();
        }
    }

    public void reset() {
        this.isSyncing.set(false);
        this.idleStrategy.reset();
    }
}
