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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;

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
    void shouldGiveSaneSystemStatus() {
        runQuery("CALL gds.test.pl('foo')");
        scheduler.forward(100, TimeUnit.MILLISECONDS);

        assertCypherResult(
            "CALL gds.alpha.systemMonitor()",
            List.of(Map.of(
                "jvmFreeMemory",
                greaterThan(0L),
                "jvmTotalMemory",
                greaterThan(0L),
                "jvmMaxMemory",
                greaterThan(0L),
                "jvmAvailableProcessors",
                greaterThan(0L),
                "jvmStatusDescription",
                aMapWithSize(4),
                "ongoingGdsProcedures",
                List.of(Map.of("taskName", "foo", "progress", "33.33%"))
            ))
        );
    }
}
