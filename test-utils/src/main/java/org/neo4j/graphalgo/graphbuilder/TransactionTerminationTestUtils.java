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
package org.neo4j.graphalgo.graphbuilder;

import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.TransactionTerminatedException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class TransactionTerminationTestUtils {

    // Give tests 5 times more time when running on CI
    private static final long CI_SMEAR = 10;

    public static void assertTerminates(Consumer<TerminationFlag> algoRunner, long terminateAfterMillis, long maxDelayMillis) {
        TestTerminationFlag terminationFlag = new TestTerminationFlag();

        AtomicLong terminationTime = new AtomicLong();
        assertThrows(TransactionTerminatedException.class, () -> {
            new java.util.Timer(true).schedule(
                new java.util.TimerTask() {
                    @Override
                    public void run() {
                        terminationTime.set(System.nanoTime());
                        terminationFlag.stop();
                    }
                },
                terminateAfterMillis
            );
            algoRunner.accept(terminationFlag);
        });
        long terminatedAt = System.nanoTime();
        long terminationDelay = terminatedAt - terminationTime.get();
        long terminationDelayMillis = TimeUnit.NANOSECONDS.toMillis(terminationDelay);

        if (System.getenv("TEAMCITY_VERSION") != null || System.getenv("CI") != null || System.getenv("BUILD_ID") != null) {
            maxDelayMillis *= CI_SMEAR;
            if (maxDelayMillis < 0) {
                maxDelayMillis = Long.MAX_VALUE;
            }
        }

        assertTrue(
            terminationDelayMillis <= maxDelayMillis,
            formatWithLocale("Expected to terminate after at most %dms but took %dms", maxDelayMillis, terminationDelayMillis)
        );
    }

    private TransactionTerminationTestUtils() {}

    static class TestTerminationFlag implements TerminationFlag {
        private boolean running;

        TestTerminationFlag() {
            this.running = true;
        }

        void stop() {
            this.running = false;
        }

        @Override
        public boolean running() {
            return this.running;
        }
    }

}
