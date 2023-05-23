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
package org.neo4j.gds.utils;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;

class AutoCloseableThreadLocalTest {

    @Test
    void threadLocalsShouldLiveUntilClosed() throws InterruptedException {
        var concurrency = 4;
        var counter = new MutableLong(0);
        var threadLocal = AutoCloseableThreadLocal.withInitial(() -> new CloseableWithState(counter));

        // Spawns a ForkJoin pool, where each thread manages a CloseableWithState.
        // Closing the ForkJoin pool makes the thread eligible for GC.
        // We need to make sure that the CloseableWithState outlives the thread and is around in the close method.
        ParallelUtil.parallelForEachNode(
            10_000_000,
            concurrency, TerminationFlag.RUNNING_TRUE,
            (__) -> threadLocal.get()
        );

        // Try to run GC, so that the ForkJoin Threads can be collected.
        Thread.sleep(100);
        System.gc();

        threadLocal.close();

        assertThat(counter.getValue()).isEqualTo(concurrency);
    }


    private static final class CloseableWithState implements AutoCloseable {
        private final MutableLong counter;

        private CloseableWithState(MutableLong counter) {
            this.counter = counter;
        }

        @Override
        public void close() {
            counter.increment();
        }
    }
}
