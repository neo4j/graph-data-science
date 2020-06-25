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
package org.neo4j.graphalgo.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.Settings;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.test.TestProc;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConcurrencyConfigEETest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(TestProc.class, GraphCreateProc.class);
        runQuery("CREATE (:A)");
        runQuery("CALL gds.graph.create(" + "'myG'" + ", '*', '*')");
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(Settings.enterpriseLicensed(), true);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldAllowHighConcurrencyForEE() throws Exception {
        String query = "CALL gds.testProc.test('myG', {concurrency: 10, writeProperty: 'p'}) " +
                       "YIELD configuration " +
                       "RETURN configuration.concurrency AS concurrency";

        runQueryWithRowConsumer(query,
            row -> assertEquals(10, row.getNumber("concurrency"))
        );
    }

    @Test
    void shouldAllowHighReadConcurrencyForEE() throws Exception {
        runQuery("CALL gds.graph.create('myG2', '*', '*', {readConcurrency: 12})");
    }

    @Test
    void shouldAllowHighWriteConcurrencyForEE() throws Exception {
        String query = "CALL gds.testProc.test('myG', {writeConcurrency: 9, writeProperty: 'p'}) " +
                       "YIELD configuration " +
                       "RETURN configuration.writeConcurrency AS concurrency";

        runQueryWithRowConsumer(query,
            row -> assertEquals(9, row.getNumber("concurrency"))
        );
    }
}
