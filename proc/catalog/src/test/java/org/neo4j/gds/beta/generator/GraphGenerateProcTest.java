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
package org.neo4j.gds.beta.generator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStatsProc;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.TestSupport.assertCypherMemoryEstimation;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_DISTRIBUTION_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_NAME_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_SEED_KEY;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphGenerateProcTest extends BaseProcTest {

    private static Stream<Arguments> invalidRelationshipDistributions() {
        return Stream.of(
            Arguments.of(1L, "Expected RelationshipDistribution or String. Got Long."),
            Arguments.of(
                "'bestDistribution'",
                "RelationshipDistribution `bestDistribution` is not supported. Must be one of: ['POWER_LAW', 'RANDOM', 'UNIFORM']."
            )
        );
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphGenerateProc.class, NodeSimilarityStatsProc.class);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldThrowOnInvalidGraphName() {
        var generateQuery = "CALL gds.graph.generate('', 10, 5)";
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(generateQuery)
        );
        Throwable throwable = rootCause(ex);
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        assertEquals("`graphName` can not be null or blank, but it was ``", throwable.getMessage());
    }

    @ParameterizedTest
    @MethodSource("invalidRelationshipDistributions")
    void shouldThrowOnInvalidRelationshipDistribution(Object distribution, String error) {
        var generateQuery = formatWithLocale("CALL gds.graph.generate('test', 10, 5, {relationshipDistribution: %s})", distribution);
        Assertions.assertThatThrownBy(() -> runQuery(generateQuery))
            .isInstanceOf(QueryExecutionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasRootCauseMessage(error);
    }

    @Test
    void shouldThrowIfGraphAlreadyExists() {
        var generateQuery = "CALL gds.graph.generate('foo', 10, 5)";
        runQuery(generateQuery);
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(generateQuery)
        );
        Throwable throwable = rootCause(ex);
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        assertEquals("A graph with name 'foo' already exists.", throwable.getMessage());
    }

    @ParameterizedTest
    @MethodSource("estimations")
    void shouldWorkWithEstimate(int nodeCount, int avgDegree, MemoryRange expected) {
        String generateQ =
            "CALL gds.graph.generate( " +
            "  'g', " +
            "  $nodeCount, " +
            "  $avgDegree " +
            ")";

        runQuery(generateQ, Map.of("nodeCount", nodeCount, "avgDegree", avgDegree));

        String estimateQ =
            "CALL gds.nodeSimilarity.stats.estimate( " +
            "  'g', " +
            "  {} " +
            ") YIELD bytesMin, bytesMax, nodeCount, relationshipCount";

        assertCypherMemoryEstimation(db, estimateQ, expected, nodeCount, nodeCount * avgDegree);
    }

    @Test
    void shouldGenerateGraphWithDefaults() {
        String query = "CALL gds.graph.generate(" +
                       "    'foo', 10, 5 " +
                       ")";

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(10, row.getNumber("nodes").intValue());
                assertEquals(50, row.getNumber("relationships").intValue());
                assertEquals("foo", row.getString(RELATIONSHIP_PROPERTY_NAME_KEY));
                assertEquals("UNIFORM", row.get(RELATIONSHIP_DISTRIBUTION_KEY));
                assertEquals(Collections.emptyMap(), row.get(RELATIONSHIP_PROPERTY_KEY));
                assertNull(row.get(RELATIONSHIP_SEED_KEY));
            }
        );
    }

    @Test
    void shouldGenerateDefaultEmptySchemaWithoutProperties() {
        String query = "CALL gds.graph.generate('g', 4, 2, {relationshipDistribution: 'RANDOM'})";
        runQuery(query);

        var graph = GraphStoreCatalog.get(this.getUsername(), DatabaseId.of(this.db.databaseName()), "g").graphStore();

        assertThat(graph.schema().relationshipSchema().hasProperties()).isFalse();
        assertThat(graph.schema().relationshipSchema().get(RelationshipType.of("REL")).properties()).isEmpty();
    }

    @Test
    void shouldGenerateGraphWithRelationshipProperty() {
        var query = "CALL gds.graph.generate('test', 10, 3, " +
                    "{" +
                    "  relationshipDistribution: 'random'," +
                    "  relationshipSeed: 42," +
                    "  relationshipProperty: {" +
                    "    name:'myProperty'," +
                    "    type: 'RANDOM'," +
                    "    min : 40.0," +
                    "    max : 80.0" +
                    " }})" +
                    "YIELD nodes, relationships, relationshipProperty";

        assertCypherResult(
            query,
            List.of(Map.of(
                "nodes",
                10L,
                "relationships",
                32L,
                "relationshipProperty",
                Map.of("min", 40.0, "max", 80.0, "name", "myProperty", "type", "RANDOM")
            ))
        );
    }

    private static Stream<Arguments> estimations() {
        return Stream.of(
            Arguments.of(100, 2, MemoryRange.of(28_088, 31_288)),
            Arguments.of(100, 4, MemoryRange.of(29_688, 34_488)),
            Arguments.of(200, 4, MemoryRange.of(59_304, 68_904))
        );
    }
}
