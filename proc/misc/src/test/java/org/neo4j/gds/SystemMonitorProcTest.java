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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SystemMonitorProcTest extends BaseProgressTest {

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            ProgressTestProc.class,
            SystemMonitorProc.class
        );
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void shouldGiveProgressOfOngoingProcs() {
        runQuery("Alice", "CALL gds.test.pl('foo')");
        // Use a non-default mock memory estimation.
        runQuery("Bob", "CALL gds.test.pl('bar', true)");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        assertCypherResult(
            "CALL gds.alpha.systemMonitor()",
            List.of(Map.of(
                "freeHeap",
                greaterThan(0L),
                "totalHeap",
                greaterThan(0L),
                "maxHeap",
                greaterThan(0L),
                "jvmAvailableProcessors",
                greaterThan(0L),
                "jvmStatusDescription",
                aMapWithSize(4),
                "ongoingGdsProcedures",
                containsInAnyOrder(
                    Map.of("taskName",
                        "foo",
                        "progress",
                        "33.33%",
                        "maxMemoryEstimation",
                        "n/a"),
                    Map.of(
                        "taskName",
                        "bar",
                        "progress",
                        "33.33%",
                        "maxMemoryEstimation",
                        MAX_MEMORY_USAGE.getAsLong() + " Bytes"
                    )
                )
            ))
        );
    }

    @Test
    @GdsEditionTest(Edition.EE)
    void shouldGiveSaneJvmStatus() {
        assertCypherResult(
            "CALL gds.alpha.systemMonitor()",
            List.of(Map.of(
                "freeHeap",
                greaterThan(0L),
                "totalHeap",
                greaterThan(0L),
                "maxHeap",
                greaterThan(0L),
                "jvmAvailableProcessors",
                greaterThan(0L),
                "jvmStatusDescription",
                aMapWithSize(4),
                "ongoingGdsProcedures",
                Matchers.empty()
            ))
        );
    }

    @Test
    @GdsEditionTest(Edition.CE)
    void shouldFailOnCommunityEdition() {
        assertThrows(RuntimeException.class,() -> runQuery("CALL gds.alpha.systemMonitor()" ));
    }
}
