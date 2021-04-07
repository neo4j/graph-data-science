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
package org.neo4j.graphalgo.catalog;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.compat.GraphStoreExportSettings;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.gdl.ImmutableGraphCreateFromGdlConfig;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class GraphStorePersistProcTest extends BaseProcTest {

    static String DB_CYPHER = "CREATE" +
                              "  (a:Label1 {prop1: 42})" +
                              ", (b:Label1)" +
                              ", (c:Label2 {prop2: 1337})" +
                              ", (d:Label2 {prop2: 10})" +
                              ", (e:Label2)" +
                              ", (a)-[:REL1]->(b)" +
                              ", (c)-[:REL2]->(d)";

    @TempDir
    Path tempDir;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphStoreExportProc.class);

        runQuery(DB_CYPHER);

        var graphStore1 = new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(NodeProjection.of("Label1", PropertyMappings.of(PropertyMapping.of("prop1"))))
            .addRelationshipProjection(RelationshipProjection.of("REL1", Orientation.NATURAL))
            .build()
            .graphStore();
        var createConfig1 = ImmutableGraphCreateFromGdlConfig
            .builder()
            .gdlGraph("")
            .graphName("first")
            .build();
        GraphStoreCatalog.set(createConfig1, graphStore1);

        var graphStore2 = new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(NodeProjection.of("Label2", PropertyMappings.of(PropertyMapping.of("prop2"))))
            .addRelationshipProjection(RelationshipProjection.of("REL2", Orientation.NATURAL))
            .build()
            .graphStore();
        var createConfig2 = ImmutableGraphCreateFromGdlConfig
            .builder()
            .gdlGraph("")
            .graphName("second")
            .build();
        GraphStoreCatalog.set(createConfig2, graphStore2);
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        builder.setConfig(GraphStoreExportSettings.export_location_setting, tempDir);
    }

    @Test
    void shouldPersistGraphStores() {
        var persistQuery =
            "CALL gds.graphs.persist({ writeConcurrency: 1 })" +
            "YIELD *";

        assertCypherResult(persistQuery, List.of(
            Map.of(
                "exportName", "first",
                "graphName", "first",
                "nodeCount", 2L,
                "relationshipCount", 1L,
                "relationshipTypeCount", 1L,
                "nodePropertyCount", 2L,
                "relationshipPropertyCount", 0L,
                "writeMillis", Matchers.greaterThan(0L)
            ),
            Map.of(
                "exportName", "second",
                "graphName", "second",
                "nodeCount", 3L,
                "relationshipCount", 1L,
                "relationshipTypeCount", 1L,
                "nodePropertyCount", 3L,
                "relationshipPropertyCount", 0L,
                "writeMillis", Matchers.greaterThan(0L)
            )
        ));
    }
}
