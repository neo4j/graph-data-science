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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;

@ValueClass
public interface RunWithConcurrency {

    long DEFAULT_WAIT_TIME_NANOS = 1000;
    long DEFAULT_MAX_NUMBER_OF_RETRIES = (long) 2.5e11; // about 3 days in micros

    int concurrency();

    Iterator<? extends Runnable> tasks();

    @Value.Default
    default long waitNanos() {
        return DEFAULT_WAIT_TIME_NANOS;
    }

    @Value.Default
    default long maxWaitRetries() {
        return DEFAULT_MAX_NUMBER_OF_RETRIES;
    }

    @Value.Default
    default boolean mayInterruptIfRunning() {
        return false;
    }

    @Value.Default
    default TerminationFlag terminationFlag() {
        return TerminationFlag.RUNNING_TRUE;
    }

    @Value.Default
    default ExecutorService executor() {
        return Pools.DEFAULT;
    }

    default void run() {
        ParallelUtil.runWithConcurrency(this);
    }

    static Builder builder() {
        return new Builder();
    }

    final class Builder extends ImmutableRunWithConcurrency.Builder {
        public Builder tasks(Iterable<? extends Runnable> tasks) {
            return this.tasks(tasks.iterator());
        }

        public void run() {
            this.build().run();
        }
    }
}
