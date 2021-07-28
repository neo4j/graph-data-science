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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.compat.StorageEngineProxy;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.compat.Neo4jVersion;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.junit.annotation.DisableForNeo4jVersion;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.storage_engine;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@EphemeralNeo4jLayoutExtension
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InMemoryDatabaseCypherTest {

    static final String DB_CYPHER = "CREATE " +
                                    "  (a:Foo { prop: 1 }), " +
                                    "  (b:Foo { prop: 2 }), " +
                                    "  (c:Bar { prop: 3 }), " +
                                    "  (a)-[:REL1 { relProp: 42.0 }]->(b)," +
                                    "  (b)-[:REL2 { relProp: 1337.0 }]->(c)";

    @Inject
    DatabaseLayout databaseLayout;

    static DatabaseManagementService dbms;
    static GraphDatabaseAPI db;
    static GraphStore graphStore;

    @BeforeAll
    void setup() {
        dbms = new EnterpriseDatabaseManagementServiceBuilder(databaseLayout.databaseDirectory())
            .setConfig(GraphDatabaseSettings.fail_on_missing_files, false)
            .setConfig(GraphDatabaseInternalSettings.skip_default_indexes_on_creation, true)
            .build();

        var neo4jDb = dbms.database(DEFAULT_DATABASE_NAME);
        neo4jDb.executeTransactionally(DB_CYPHER);
        graphStore = new StoreLoaderBuilder()
            .api((GraphDatabaseAPI) neo4jDb)
            .addNodeLabels("Foo", "Bar")
            .addRelationshipTypes("REL1", "REL2")
            .addNodeProperty(PropertyMapping.of("prop"))
            .addRelationshipProperty(PropertyMapping.of("relProp"))
            .build()
            .graphStore();

        GraphStoreCatalog.set(GraphCreateFromStoreConfig.emptyWithName("", "gds"), graphStore);

        var config = Config.defaults();
        config.set(storage_engine, StorageEngineProxy.inMemoryStorageEngineFactoryName());
        dbms.createDatabase("gds", config);
        dbms.startDatabase("gds");
        db = (GraphDatabaseAPI) dbms.database("gds");
    }

    @AfterAll
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
        dbms.shutdown();
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_0)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    void shouldHaveStartedSuccessfully() {
        assertThat(db.isAvailable(1000)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"Foo", "Bar"})
    @DisableForNeo4jVersion(Neo4jVersion.V_4_0)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    void shouldReturnNodeForSpecificLabel(String label) {
        String query = formatWithLocale("MATCH (n:%s) RETURN id(n) AS id", label);

        List<Long> foundIds = new ArrayList<>();
        db.executeTransactionally(query, Map.of(), result -> {
            result.accept(row -> {
                foundIds.add(row.getNumber("id").longValue());

                return true;
            });
            return true;
        });

        var expectedIds = LongStream
            .range(0, graphStore.nodeCount())
            .filter(id -> graphStore.nodes().nodeLabels(id).contains(NodeLabel.of(label)))
            .boxed()
            .toArray(Long[]::new);

        assertThat(foundIds).containsExactlyInAnyOrder(expectedIds);
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_0)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    void shouldReturnNodeProperties() {
        var graph = graphStore.getUnion();

        String query = "MATCH (n) RETURN id(n) AS id, n.prop AS prop";
        db.executeTransactionally(query, Map.of(), result -> {
            result.accept(row -> {
                var expected = graph.nodeProperties("prop").longValue(row.getNumber("id").longValue());
                var actual = row.getNumber("prop").longValue();

                assertThat(actual).isEqualTo(expected);
                return true;
            });
            return true;
        });
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_0)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    void shouldReturnFilteredNodeProperties() {
        String query = "MATCH (n) WHERE id(n) = 2 RETURN id(n) AS id, n.prop AS prop";
        db.executeTransactionally(query, Map.of(), result -> {
            result.accept(row -> {
                var expected = graphStore.nodePropertyValues("prop").longValue(row.getNumber("id").longValue());
                var actual = row.getNumber("prop").longValue();

                assertThat(actual).isEqualTo(expected);
                return true;
            });
            return true;
        });
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_0)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    void shouldReturnCorrectPropertiesForMultipleIdenticalNodes() {
        String query = "MATCH (a), (b), (c) WHERE id(a) = 0 AND id(b) = 0 AND id(c) = 1 RETURN a.prop, b.prop, c.prop";
        db.executeTransactionally(query, Map.of(), result -> {
            result.accept(row -> {
                assertThat(row.getNumber("a.prop").longValue()).isEqualTo(graphStore.nodePropertyValues("prop").longValue(0));
                assertThat(row.getNumber("b.prop").longValue()).isEqualTo(graphStore.nodePropertyValues("prop").longValue(0));
                assertThat(row.getNumber("c.prop").longValue()).isEqualTo(graphStore.nodePropertyValues("prop").longValue(1));

                return true;
            });
            return true;
        });
    }

    @Test
    @DisableForNeo4jVersion(Neo4jVersion.V_4_0)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_1)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_2)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop31)
    @DisableForNeo4jVersion(Neo4jVersion.V_4_3_drop40)
    void shouldFilterRelationshipTypes() {
        var rel1Graph = graphStore.getGraph(RelationshipType.of("REL1"));
        var rel2Graph = graphStore.getGraph(RelationshipType.of("REL2"));

        String rel1Query = "MATCH (a)-[r:REL1]->(b) RETURN id(a) AS a, id(b) AS b";
        db.executeTransactionally(rel1Query, Map.of(), result -> {
            result.accept(row -> {
                long source = row.getNumber("a").longValue();
                long target = row.getNumber("b").longValue();

                List<Long> expected = rel1Graph
                    .streamRelationships(source, Double.NaN)
                    .map(RelationshipCursor::targetId)
                    .collect(Collectors.toList());

                assertThat(List.of(target)).isEqualTo(expected);
                return true;
            });
            return true;
        });

        String rel2Query = "MATCH (a)-[r:REL2]->(b) RETURN id(a) AS a, id(b) AS b";
        db.executeTransactionally(rel2Query, Map.of(), result -> {
            result.accept(row -> {
                long source = row.getNumber("a").longValue();
                long target = row.getNumber("b").longValue();

                List<Long> expected = rel2Graph
                    .streamRelationships(source, Double.NaN)
                    .map(RelationshipCursor::targetId)
                    .collect(Collectors.toList());

                assertThat(List.of(target)).isEqualTo(expected);
                return true;
            });
            return true;
        });
    }
}
