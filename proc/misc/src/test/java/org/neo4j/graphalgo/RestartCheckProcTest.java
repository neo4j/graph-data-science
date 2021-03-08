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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.internal.kernel.api.security.AuthSubject;

import java.util.List;
import java.util.Map;

class RestartCheckProcTest extends BaseTest {

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            RestartCheckProc.class
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void safeToRestartReturnsTrueWhenCatalogIsEmpty() {
        assertCypherResult("CALL gds.internal.safeToRestart()", List.of(
            Map.of("safeToRestart", Boolean.TRUE)
        ));
    }

    @Test
    void safeToRestartReturnsFalseIfThereAreGraphsInTheCatalog() {
        var config = ImmutableGraphCreateFromStoreConfig.of(
            AuthSubject.ANONYMOUS.username(),
            "config",
            NodeProjections.all(),
            RelationshipProjections.all()
        );
        var graphStore = new StoreLoaderBuilder().api(db)
            .build()
            .graphStore();
        GraphStoreCatalog.set(config, graphStore);

        assertCypherResult("CALL gds.internal.safeToRestart()", List.of(
            Map.of("safeToRestart", Boolean.FALSE)
        ));
    }
}
