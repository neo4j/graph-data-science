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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.functions.AsNodeFunc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.values.storable.NumberType;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class GraphStreamRelationshipPropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";
    private static final String TEST_GRAPH_DIFFERENT_PROPERTIES = "testGraph2";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label { id: 0 })" +
        ", (b:Label { id: 1 })" +
        ", (a)-[:REL1 { relProp1: 0.0, relProp2: 42.0}]->(a)" +
        ", (b)-[:REL1 { relProp1: 1.0, relProp2: 43.0}]->(b)" +
        ", (a)-[:REL2 { relProp1: 2.0, relProp2: 44.0}]->(a)" +
        ", (b)-[:REL2 { relProp1: 3.0, relProp2: 45.0}]->(b)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, GraphStreamRelationshipPropertiesProc.class);
        registerFunctions(AsNodeFunc.class);
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("REL1")
            .withRelationshipType("REL2")
            .withRelationshipProperty("relProp1")
            .withRelationshipProperty("relProp2")
            .graphCreate(TEST_GRAPH_SAME_PROPERTIES)
            .yields()
        );

        runQuery(GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("REL1", RelationshipProjection.builder()
                .type("REL1")
                .addProperties(
                    PropertyMapping.of("newRelProp1", "relProp1", 1337),
                    PropertyMapping.of("newRelProp2", "relProp2", 1337)
                )
                .build()
            )
            .withRelationshipType("REL2", RelationshipProjection.builder()
                .type("REL2")
                .addProperties(PropertyMapping.of("newRelProp1", "relProp1", 1337))
                .build()
            )
            .graphCreate(TEST_GRAPH_DIFFERENT_PROPERTIES)
            .yields());
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // no labels -> defaults to PROJECT_ALL
        "CALL gds.graph.streamRelationshipProperties(" +
        "   '%s', " +
        "   ['relProp1', 'relProp2']" +
        ") YIELD sourceNodeId, targetNodeId, relationshipType, relationshipProperty, propertyValue " +
        "RETURN gds.util.asNode(sourceNodeId).id AS source, gds.util.asNode(targetNodeId).id AS target, relationshipType, relationshipProperty, propertyValue",
        // explicit PROJECT_ALL
        "CALL gds.graph.streamRelationshipProperties(" +
        "   '%s', " +
        "   ['relProp1', 'relProp2'], " +
        "   ['*']" +
        ") YIELD sourceNodeId, targetNodeId, relationshipType, relationshipProperty, propertyValue " +
        "RETURN gds.util.asNode(sourceNodeId).id AS source, gds.util.asNode(targetNodeId).id AS target, relationshipType, relationshipProperty, propertyValue"
    })
    void streamLoadedRelationshipProperties(String graphStreamQueryTemplate) {
        String graphStreamQuery = formatWithLocale(graphStreamQueryTemplate, TEST_GRAPH_SAME_PROPERTIES);

        assertCypherResult(graphStreamQuery, List.of(
            map("source", 0L, "target", 0L, "relationshipType", "REL1", "relationshipProperty", "relProp1", "propertyValue", 0D),
            map("source", 0L, "target", 0L, "relationshipType", "REL1", "relationshipProperty", "relProp2", "propertyValue", 42D),
            map("source", 0L, "target", 0L, "relationshipType", "REL2", "relationshipProperty", "relProp1", "propertyValue", 2D),
            map("source", 0L, "target", 0L, "relationshipType", "REL2", "relationshipProperty", "relProp2", "propertyValue", 44D),

            map("source", 1L, "target", 1L, "relationshipType", "REL1", "relationshipProperty", "relProp1", "propertyValue", 1D),
            map("source", 1L, "target", 1L, "relationshipType", "REL1", "relationshipProperty", "relProp2", "propertyValue", 43D),
            map("source", 1L, "target", 1L, "relationshipType", "REL2", "relationshipProperty", "relProp1", "propertyValue", 3D),
            map("source", 1L, "target", 1L, "relationshipType", "REL2", "relationshipProperty", "relProp2", "propertyValue", 45D)
        ));
    }

    @Test
    void streamLoadedRelationshipPropertiesForType() {
        String graphStreamQuery = formatWithLocale(
            "CALL gds.graph.streamRelationshipProperties(" +
            "   '%s', " +
            "   ['relProp1', 'relProp2'], " +
            "   ['REL1']" +
            ") YIELD sourceNodeId, targetNodeId, relationshipType, relationshipProperty, propertyValue " +
            "RETURN gds.util.asNode(sourceNodeId).id AS source, gds.util.asNode(targetNodeId).id AS target, relationshipType, relationshipProperty, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphStreamQuery, List.of(
            map("source", 0L, "target", 0L, "relationshipType", "REL1", "relationshipProperty", "relProp1", "propertyValue", 0D),
            map("source", 0L, "target", 0L, "relationshipType", "REL1", "relationshipProperty", "relProp2", "propertyValue", 42D),
            map("source", 1L, "target", 1L, "relationshipType", "REL1", "relationshipProperty", "relProp1", "propertyValue", 1D),
            map("source", 1L, "target", 1L, "relationshipType", "REL1", "relationshipProperty", "relProp2", "propertyValue", 43D)
        ));
    }

    @Test
    void streamLoadedRelationshipPropertiesForTypeSubset() {
        String graphStreamQuery = formatWithLocale(
            "CALL gds.graph.streamRelationshipProperties(" +
            "   '%s', " +
            "   ['newRelProp1', 'newRelProp2']" +
            ") YIELD sourceNodeId, targetNodeId, relationshipType, relationshipProperty, propertyValue " +
            "RETURN gds.util.asNode(sourceNodeId).id AS source, gds.util.asNode(targetNodeId).id AS target, relationshipType, relationshipProperty, propertyValue",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        assertCypherResult(graphStreamQuery, List.of(
            map("source", 0L, "target", 0L, "relationshipType", "REL1", "relationshipProperty", "newRelProp1", "propertyValue", 0D),
            map("source", 0L, "target", 0L, "relationshipType", "REL1", "relationshipProperty", "newRelProp2", "propertyValue", 42D),
            map("source", 0L, "target", 0L, "relationshipType", "REL2", "relationshipProperty", "newRelProp1", "propertyValue", 2D),

            map("source", 1L, "target", 1L, "relationshipType", "REL1", "relationshipProperty", "newRelProp1", "propertyValue", 1D),
            map("source", 1L, "target", 1L, "relationshipType", "REL1", "relationshipProperty", "newRelProp2", "propertyValue", 43D),
            map("source", 1L, "target", 1L, "relationshipType", "REL2", "relationshipProperty", "newRelProp1", "propertyValue", 3D)
        ));
    }

    @Test
    void streamMutatedRelationshipProperties() {
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), TEST_GRAPH_SAME_PROPERTIES).graphStore();

        RelationshipsBuilder relImporter = GraphFactory.initRelationshipsBuilder()
            .nodes(graphStore.nodes())
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(Aggregation.NONE, DefaultValue.forDouble())
            .build();

        relImporter.addFromInternal(0, 1, 23D);

        graphStore.addRelationshipType(RelationshipType.of("NEW_REL"), Optional.of("newRelProp3"), Optional.of(NumberType.FLOATING_POINT), relImporter.build());

        String graphStreamQuery = formatWithLocale(
            "CALL gds.graph.streamRelationshipProperties(" +
            "   '%s', " +
            "   ['newRelProp3']" +
            ") YIELD sourceNodeId, targetNodeId, relationshipType, relationshipProperty, propertyValue " +
            "RETURN gds.util.asNode(sourceNodeId).id AS source, gds.util.asNode(targetNodeId).id AS target, relationshipType, relationshipProperty, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphStreamQuery, List.of(
            map("source", 0L, "target", 1L, "relationshipType", "NEW_REL", "relationshipProperty", "newRelProp3", "propertyValue", 23D)
        ));
    }

    @Test
    void shouldFailOnNonExistingRelationshipProperties() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.streamRelationshipProperties(" +
                "   '%s', " +
                "   ['newRelProp1', 'newRelProp2', 'newRelProp3']" +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(
            rootCause.getMessage(),
            containsString("No relationship projection with property key(s) ['newRelProp1', 'newRelProp2', 'newRelProp3'] found")
        );
    }

    @Test
    void shouldFailOnNonExistingRelationshipPropertiesForSpecificType() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.streamRelationshipProperties(" +
                "   '%s', " +
                "   ['relProp1', 'relProp2', 'relProp3'], " +
                "   ['REL1'] " +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Relationship projection 'REL1' does not have property key 'relProp3'"));
        assertThat(rootCause.getMessage(), containsString("Available keys: ['relProp1', 'relProp2']"));
    }

    @Test
    void streamLoadedRelationshipPropertyForType() {
        String graphStreamQuery = formatWithLocale(
            "CALL gds.graph.streamRelationshipProperty(" +
            "   '%s', " +
            "   'relProp1', " +
            "   ['REL1']" +
            ") YIELD sourceNodeId, targetNodeId, relationshipType, propertyValue " +
            "RETURN gds.util.asNode(sourceNodeId).id AS source, gds.util.asNode(targetNodeId).id AS target, relationshipType, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphStreamQuery, List.of(
            map("source", 0L, "target", 0L, "relationshipType", "REL1", "propertyValue", 0D),
            map("source", 1L, "target", 1L, "relationshipType", "REL1", "propertyValue", 1D)
        ));
    }

    @Test
    void streamLoadedRelationshipPropertyForTypeSubset() {
        String graphStreamQuery = formatWithLocale(
            "CALL gds.graph.streamRelationshipProperty(" +
            "   '%s', " +
            "   'newRelProp2'" +
            ") YIELD sourceNodeId, targetNodeId, relationshipType, propertyValue " +
            "RETURN gds.util.asNode(sourceNodeId).id AS source, gds.util.asNode(targetNodeId).id AS target, relationshipType, propertyValue",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        assertCypherResult(graphStreamQuery, List.of(
            map("source", 0L, "target", 0L, "relationshipType", "REL1", "propertyValue", 42D),
            map("source", 1L, "target", 1L, "relationshipType", "REL1", "propertyValue", 43D)
        ));
    }

    @Test
    void streamMutatedNodeProperty() {
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), TEST_GRAPH_SAME_PROPERTIES).graphStore();

        RelationshipsBuilder relImporter = GraphFactory.initRelationshipsBuilder()
            .nodes(graphStore.nodes())
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(Aggregation.NONE, DefaultValue.forDouble())
            .build();

        relImporter.addFromInternal(0, 1, 23D);

        graphStore.addRelationshipType(RelationshipType.of("NEW_REL"), Optional.of("newRelProp3"), Optional.of(NumberType.FLOATING_POINT), relImporter.build());

        String graphStreamQuery = formatWithLocale(
            "CALL gds.graph.streamRelationshipProperty(" +
            "   '%s', " +
            "   'newRelProp3'" +
            ") YIELD sourceNodeId, targetNodeId, relationshipType, propertyValue " +
            "RETURN gds.util.asNode(sourceNodeId).id AS source, gds.util.asNode(targetNodeId).id AS target, relationshipType, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphStreamQuery, List.of(
            map("source", 0L, "target", 1L, "relationshipType", "NEW_REL", "propertyValue", 23D)
        ));
    }

    @Test
    void shouldFailOnNonExistingRelationshipProperty() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.streamRelationshipProperty(" +
                "   '%s', " +
                "   'relProp3'" +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(
            rootCause.getMessage(),
            containsString("No relationship projection with property key(s) ['relProp3'] found")
        );
    }

    @Test
    void shouldFailOnNonExistingRelationshipPropertyForSpecificType() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.streamRelationshipProperty(" +
                "   '%s', " +
                "   'relProp3', " +
                "   ['REL1'] " +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Relationship projection 'REL1' does not have property key 'relProp3'"));
        assertThat(rootCause.getMessage(), containsString("Available keys: ['relProp1', 'relProp2']"));
    }

    @Test
    void shouldFailOnDisjunctCombinationsOfRelationshipTypeAndProperty() {
        String graphStreamQuery = formatWithLocale(
            "CALL gds.graph.streamRelationshipProperties(" +
            "   '%s', " +
            "   ['newRelProp1', 'newRelProp2']," +
            "   ['REL2']" +
            ") YIELD sourceNodeId, targetNodeId, relationshipType, relationshipProperty, propertyValue",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(graphStreamQuery)
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Relationship projection 'REL2' does not have property key 'newRelProp2'"));
        assertThat(rootCause.getMessage(), containsString("Available keys: ['newRelProp1']"));
    }
}
