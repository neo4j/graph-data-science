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
package org.neo4j.graphalgo.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.graphalgo.core.Settings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.memory.MemoryLimitExceededException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.graphalgo.utils.GdsFeatureToggles.USE_KERNEL_TRACKER;

public class AllocationTrackerProcTest extends BaseProcTest {

    // Small enough so the Neo4j create query doesn't exceed the limit,
    // large enough so the GDS create query does.
    private static final String DB_CYPHER = "UNWIND range(0, 4096) AS x CREATE ()";

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(Settings.memoryTransactionMaxSize(), 1024 * 1024 + 1L);
    }

    @BeforeEach
    void setUp() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(GraphCreateProc.class, AllocationTrackingTestProc.class);
    }

    @Test
    void shouldFailOnMemoryLimitExceeded() {
        String cypher = GdsCypher.call()
            .loadEverything()
            .graphCreate("foo")
            .yields();
        USE_KERNEL_TRACKER.enableAndRun(
            () -> assertThatThrownBy(() -> runQuery(cypher))
                .isInstanceOf(QueryExecutionException.class)
                .getRootCause()
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageStartingWith("The allocation of an extra")
        );
    }

    @Test
    void shouldReally() {
        String cypher = "CALL test.doIt()";
        USE_KERNEL_TRACKER.enableAndRun(
            () -> assertThatThrownBy(
                () -> SecureTransaction.of(db).accept((tx, ktx) -> tx.execute(cypher).next())
            )
        );
    }
}
