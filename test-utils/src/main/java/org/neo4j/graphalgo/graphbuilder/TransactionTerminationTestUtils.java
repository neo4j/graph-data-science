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

package org.neo4j.graphalgo.graphbuilder;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.TransactionTerminatedException;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class TransactionTerminationTestUtils {

    public static void assertTerminates(Consumer<TerminationFlag> algoRunner, long terminateAfter) {
        assertTerminates(algoRunner, terminateAfter, Long.MAX_VALUE);
    }

    public static void assertTerminates(Consumer<TerminationFlag> algoRunner, long terminateAfter, long maxDelay) {
        TestTerminationFlag terminationFlag = new TestTerminationFlag();

        MutableLong terminationTime = new MutableLong();
        assertThrows(TransactionTerminatedException.class, () -> {
            new java.util.Timer().schedule(
                new java.util.TimerTask() {
                    @Override
                    public void run() {
                        terminationTime.setValue(System.currentTimeMillis());
                        terminationFlag.stop();
                    }
                },
                terminateAfter
            );
            algoRunner.accept(terminationFlag);
        });
        long terminatedAt = System.currentTimeMillis();
        long terminationDelay = terminatedAt - terminationTime.getValue();

        assertTrue(terminationDelay <= maxDelay, String.format("Expected to terminate after at most %dms but took %dms", maxDelay, terminationDelay));
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
