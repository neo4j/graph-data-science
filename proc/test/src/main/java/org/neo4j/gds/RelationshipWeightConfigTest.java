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

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.QueryRunner.runQuery;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.config.AlgoBaseConfig.NODE_LABELS_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

public interface RelationshipWeightConfigTest<ALGORITHM extends Algorithm<ALGORITHM, RESULT>, CONFIG extends RelationshipWeightConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    String CREATE_QUERY = "CREATE" +
                          "  (x: Ignore)" +
                          ", (a: Label)" +
                          ", (b: Label)" +
                          ", (c: Label)" +
                          ", (y: Ignore)" +
                          ", (z: Ignore)" +
                          ", (a)-[:TYPE { weight1: 0.0, weight2: 1.0 }]->(b)" +
                          ", (a)-[:TYPE { weight2: 1.0 }]->(c)" +
                          ", (b)-[:TYPE { weight1: 0.0 }]->(c)" +
                          ", (c)-[:TYPE1 { weight1: 0.0 }]->(a)" +
                          ", (x)-[:TYPE]->(z)" +
                          ", (y)-[:TYPE]->(a)";

    @Test
    default void testDefaultRelationshipWeightPropertyIsNull() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.empty();
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.relationshipWeightProperty());
    }

    @Test
    default void testRelationshipWeightPropertyFromConfig() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(map("relationshipWeightProperty", "weight"));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertEquals("weight", config.relationshipWeightProperty());
    }

    @Test
    default void testEmptyRelationshipWeightPropertyValues() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(map("relationshipWeightProperty", null));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.relationshipWeightProperty());
    }

    @Test
    default void testTrimmedToNullRelationshipWeightProperty() {
        CypherMapWrapper mapWrapper = CypherMapWrapper.create(MapUtil.map("relationshipWeightProperty", "  "));
        CONFIG config = createConfig(createMinimalConfig(mapWrapper));
        assertNull(config.relationshipWeightProperty());
    }

    @Test
    default void testRelationshipWeightPropertyValidation() {
        runQuery(graphDb(), "CREATE ()-[:A {a: 1}]->()");
        var graphName = "oneRelWeightsGraph";
        var relationshipProperties = PropertyMappings.of(singletonList(PropertyMapping.of("a")));
        var relationshipProjections = RelationshipProjections.of(Map.of(
            RelationshipType.of("A"),
            RelationshipProjection.of(
                "A",
                Orientation.NATURAL,
                Aggregation.DEFAULT
            ).withProperties(relationshipProperties)
            )
        );
        loadExplicitGraphWithRelationshipWeights(graphName, NodeProjections.ALL,relationshipProjections);

        Map<String, Object> config = createMinimalConfig(CypherMapWrapper
            .empty()
            .withString("relationshipWeightProperty", "foo")).toMap();

        applyOnProcedure(proc -> {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> proc.compute(graphName, config)
            );
            assertThat(e.getMessage(), containsString("foo"));
            assertThat(e.getMessage(), containsString("['a']"));
        });
    }

    @Test
    default void shouldIndicateWhichRelationshipHasAMissingProperty() {
        runQuery(graphDb(), "MATCH (n) DETACH DELETE n");

        runQuery(graphDb(), "CREATE" +
                            "  (a:Node)" +
                            ", (b:Node)" +
                            ", (a)-[:Type {t: 3} ]->(b)" +
                            ", (a)-[:TypeWithProp {t: 4, prop: 42}]->(b)");

        String loadedGraphName = "loadedGraph";

        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(graphDb())
            .graphName(loadedGraphName)
            .addRelationshipProjection(RelationshipProjection.builder()
                .type("Type")
                .addProperty("t", "t", DefaultValue.of(0))
                .build())
            .addRelationshipProjection(RelationshipProjection.builder()
                .type("TypeWithProp")
                .addProperty("prop", "prop", DefaultValue.of(0))
                .addProperty("t", "t", DefaultValue.of(0))
                .build()
            ).build();

        GraphStoreCatalog.set(graphLoader.createConfig(), graphLoader.graphStore());

        applyOnProcedure((proc) -> {

            CypherMapWrapper mapWrapper = CypherMapWrapper.create(map(
                "relationshipWeightProperty", "prop"
            ));
            Map<String, Object> configMap = createMinimalConfig(mapWrapper).toMap();
            String error = "Relationship weight property `prop` not found in relationship types ['Type']. Properties existing on all relationship types: ['t']";
            assertMissingProperty(error, () -> proc.compute(
                loadedGraphName,
                configMap
            ));
        });
    }

    @Test
    default void testRunUnweightedOnWeightedImplicitlyLoadedGraph() {
        if (supportsImplicitGraphCreate()) {
            runQuery(graphDb(), "MATCH (n) DETACH DELETE n");
            runQuery(graphDb(), CREATE_QUERY);

            String labelString = "Label";

            CypherMapWrapper weightConfig = CypherMapWrapper.create(map(
                NODE_PROJECTION_KEY, NodeProjections.builder()
                    .putProjection(NodeLabel.of(labelString), NodeProjection.of(labelString, PropertyMappings.of()))
                    .build(),
                RELATIONSHIP_PROJECTION_KEY, "*",
                "relationshipProperties", "weight1"
            ));
            CypherMapWrapper algoConfig = createMinimalConfig(weightConfig);

            applyOnProcedure((proc) -> {
                CONFIG config = proc.newConfig(Optional.empty(), algoConfig);
                Pair<CONFIG, Optional<String>> configAndName = Tuples.pair(config, Optional.empty());
                Graph graph = proc.createGraph(configAndName);
                assertGraphEquals(fromGdl("(a:Label)-->(b:Label)-->(c:Label)-->(a)-->(c)"), graph);
            });
        }
    }

    default void loadExplicitGraphWithRelationshipWeights(String graphName, NodeProjections nodeProjections, RelationshipProjections relationshipProjections) {
        GraphDatabaseAPI db = emptyDb();

        try {
            GraphDatabaseApiProxy.registerProcedures(db, GraphCreateProc.class);
        } catch (Exception ke) {}

        runQuery(db, CREATE_QUERY);

        GraphCreateConfig graphCreateConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .graphName(graphName)
            .nodeProjections(nodeProjections)
            .relationshipProjections(relationshipProjections)
            .build();

        GraphStore graphStore = graphLoader(db, graphCreateConfig).graphStore();

        GraphStoreCatalog.set(graphCreateConfig, graphStore);
    }

    default CypherMapWrapper createMinimalConfigWithFilteredNodes(CypherMapWrapper config) {
        return createMinimalConfig(config).withEntry(NODE_LABELS_KEY, Collections.singletonList("Label"));
    }
}
