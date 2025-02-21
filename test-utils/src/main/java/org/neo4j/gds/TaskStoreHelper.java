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
package org.neo4j.gds;

import org.neo4j.gds.core.utils.progress.TaskStore;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TaskStoreHelper {

    public static void awaitEmptyTaskStore(TaskStore taskStore) {
        // there is a race condition between the thread consuming the result,
        // and the thread scheduled to end the last subtask
        long timeoutInSeconds = 5 * (TestSupport.CI ? 5 : 1);
        var deadline = Instant.now().plus(timeoutInSeconds, ChronoUnit.SECONDS);

        // On my machine (TM) with 1000 iterations this never fails and each run is 10ish or 100ish ms
        while (Instant.now().isBefore(deadline)) {
            if (taskStore.queryRunning().findAny().isEmpty()) {
                break;
            }

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
    }
}
