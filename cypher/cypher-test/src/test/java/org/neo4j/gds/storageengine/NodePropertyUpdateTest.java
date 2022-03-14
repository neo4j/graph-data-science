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
package org.neo4j.gds.storageengine;

import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.Settings;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;


@EphemeralNeo4jLayoutExtension
class NodePropertyUpdateTest extends BaseTest {

    static final String DB_CYPHER = "CREATE" +
                                    "  (a:A {prop1: 42})" +
                                    ", (b:B {prop2: 1337})" +
                                    ", (:A)" +
                                    ", (:A)";

    @Inject
    DatabaseLayout databaseLayout;

    private GraphDatabaseService inMemoryDb;
    private GraphStore graphStore;

    @BeforeEach
    void setup() {
        var dbms = new EnterpriseDatabaseManagementServiceBuilder(databaseLayout.databaseDirectory())
            .setConfig(Settings.onlineBackupEnabled(), false)
            .setConfig(Settings.boltEnabled(), false)
            .setConfig(Settings.httpEnabled(), false)
            .setConfig(Settings.httpsEnabled(), false)
            .build();
        db = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);

        QueryRunner.runQuery(db, DB_CYPHER);

        graphStore = new StoreLoaderBuilder()
             .api(db)
             .addNodeProjection(NodeProjection.of("A", PropertyMappings.of(PropertyMapping.of("prop1"))))
             .addNodeProjection(NodeProjection.of("B", PropertyMappings.of(PropertyMapping.of("prop2"))))
             .build()
             .graphStore();

        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("", "graph"), graphStore);

        InMemoryDatabaseCreator.createDatabase(db, "", db.databaseId(), "graph", "graph");

        inMemoryDb = dbms.database("graph");
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_4_drop10)
    void shouldCreateNewNodeProperties() {
        try(var tx = inMemoryDb.beginTx()) {
            for (Node node : tx.getAllNodes()) {
                node.setProperty("id", node.getId());
            }
            tx.commit();
        }

        for (NodeLabel nodeLabel : graphStore.nodeLabels()) {
            assertThat(graphStore.hasNodeProperty(nodeLabel, "id")).isTrue();
        }

        for (long i = 0; i < graphStore.nodeCount(); i++) {
            assertThat(graphStore.nodePropertyValues("id").longValue(i)).isEqualTo(i);
        }
    }
}
