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
package org.neo4j.gds.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

class AuraShutdownProcAsyncTest extends AuraShutdownProcTest {

    @BeforeEach
    void setup() throws Exception {
        super.setup();
        GraphDatabaseApiProxy.resolveDependency(db, GlobalProcedures.class).register(new AuraShutdownProc());
    }

    @Test
    void shouldPersistGraphStoresOnMultipleShutdownCalls() {
        var shutdownQuery = "CALL gds.internal.shutdown()";

        assertCypherResult(shutdownQuery, List.of(Map.of("submitted", true)));
        assertCypherResult(shutdownQuery, List.of(Map.of("submitted", true)));

        var graphsPath = tempDir.resolve("graphs");
        var modelsPath = tempDir.resolve("models");

        while (!Files.exists(graphsPath) || !Files.exists(modelsPath)) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(42));
        }

        assertSuccessfulWrite();
    }

}
