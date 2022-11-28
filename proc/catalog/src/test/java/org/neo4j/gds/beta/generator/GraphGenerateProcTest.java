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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStatsProc;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.assertCypherMemoryEstimation;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_DISTRIBUTION_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_MAX_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_MIN_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_NAME_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_TYPE_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_VALUE_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_SEED_KEY;
import static org.neo4j.gds.core.CypherMapWrapper.create;
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
        var generateQuery = "CALL gds.beta.graph.generate('', 10, 5)";
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
        var generateQuery = formatWithLocale("CALL gds.beta.graph.generate('test', 10, 5, {relationshipDistribution: %s})", distribution);
        Assertions.assertThatThrownBy(() -> runQuery(generateQuery))
            .isInstanceOf(QueryExecutionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasRootCauseMessage(error);
    }

    @Test
    void shouldThrowIfGraphAlreadyExists() {
        var generateQuery = "CALL gds.beta.graph.generate('foo', 10, 5)";
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
            "CALL gds.beta.graph.generate( " +
            "  'g', " +
            "  $nodeCount, " +
            "  $avgDegree " +
            ")";

        runQuery(generateQ, map("nodeCount", nodeCount, "avgDegree", avgDegree));

        String estimateQ =
            "CALL gds.nodeSimilarity.stats.estimate( " +
            "  'g', " +
            "  {} " +
            ") YIELD bytesMin, bytesMax, nodeCount, relationshipCount";

        assertCypherMemoryEstimation(db, estimateQ, expected, nodeCount, nodeCount * avgDegree);
    }

    @Test
    void shouldGenerateGraphWithDefaults() {
        String query = "CALL gds.beta.graph.generate(" +
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
        String query = "CALL gds.beta.graph.generate('g', 4, 2, {relationshipDistribution: 'RANDOM'})";
        runQuery(query);

        var graph = GraphStoreCatalog.get(this.getUsername(), DatabaseId.of(this.db), "g").graphStore();

        assertThat(graph.schema().relationshipSchema().hasProperties()).isFalse();
        assertThat(graph.schema().relationshipSchema().get(RelationshipType.of("REL")).properties()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(RelationshipDistribution.class)
    void shouldGenerateGraphWithRelationshipDistribution(RelationshipDistribution relationshipDistribution) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RELATIONSHIP_DISTRIBUTION_KEY, relationshipDistribution.name());

        RandomGraphGeneratorConfig cfg = RandomGraphGeneratorConfig.of(getUsername(), "", 10, 5, create(configMap));

        GraphGenerateProc proc = new GraphGenerateProc();
        RandomGraphGenerator generator = proc.initializeGraphGenerator(10, 5, cfg);

        assertEquals(relationshipDistribution, generator.getRelationshipDistribution());
        assertFalse(generator.getMaybeRelationshipPropertyProducer().isPresent());
    }

    @ParameterizedTest
    @MethodSource("relationshipPropertyProducers")
    void shouldGenerateGraphWithRelationshipProperty(
        Map<String, Object> config,
        PropertyProducer<double[]> propertyProducer
    ) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RELATIONSHIP_PROPERTY_KEY, config);

        RandomGraphGeneratorConfig cfg = RandomGraphGeneratorConfig.of(getUsername(), "", 10, 5, create(configMap));

        GraphGenerateProc proc = new GraphGenerateProc();
        RandomGraphGenerator generator = proc.initializeGraphGenerator(10, 5, cfg);

        assertTrue(generator.getMaybeRelationshipPropertyProducer().isPresent());
        PropertyProducer<double[]> actualPropertyProducer = generator.getMaybeRelationshipPropertyProducer().get();

        assertEquals(propertyProducer, actualPropertyProducer);
    }

    @Test
    void shouldGenerateGraphWithRelationshipProperty() {
        var query = "CALL gds.beta.graph.generate('test', 10, 3, " +
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
            List.of(Map.of("nodes",
                10L,
                "relationships",
                31L,
                "relationshipProperty",
                Map.of("min", 40.0, "max", 80.0, "name", "myProperty", "type", "RANDOM")
            ))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRelationshipPropertyProducers")
    void shouldThrowOnInvalidRelationshipPropertyParameters(
        String description,
        Object config,
        Iterable<String> errorFragments
    ) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RELATIONSHIP_PROPERTY_KEY, config);

        RandomGraphGeneratorConfig cfg = RandomGraphGeneratorConfig.of(getUsername(), "", 10, 5, create(configMap));

        GraphGenerateProc proc = new GraphGenerateProc();

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> proc.initializeGraphGenerator(10, 5, cfg)
        );

        String message = exception.getMessage();
        errorFragments.forEach((expectedErrorFragment) -> {
            assertTrue(
                message.contains(expectedErrorFragment),
                formatWithLocale(
                    "Expected error message to contain `%s`, but got `%s`",
                    expectedErrorFragment,
                    message
                )
            );
        });
    }

    @Test
    void shouldGenerateGraphFromDefaults() {
        Map<String, Object> configMap = new HashMap<>();

        RandomGraphGeneratorConfig cfg = RandomGraphGeneratorConfig.of(getUsername(), "", 10, 5, create(configMap));

        GraphGenerateProc proc = new GraphGenerateProc();
        RandomGraphGenerator generator = proc.initializeGraphGenerator(10, 5, cfg);

        assertEquals(generator.getRelationshipDistribution(), RelationshipDistribution.UNIFORM);
        assertFalse(generator.getMaybeRelationshipPropertyProducer().isPresent());
    }

    @Test
    void shouldBeSeedableGenerator() {
        long relationshipSeed = 4242L;

        Map<String, Object> configMap = map("relationshipSeed", relationshipSeed);

        RandomGraphGeneratorConfig cfg = RandomGraphGeneratorConfig.of(getUsername(), "", 10, 5, create(configMap));

        GraphGenerateProc proc = new GraphGenerateProc();
        RandomGraphGenerator generator = proc.initializeGraphGenerator(10, 5, cfg);
        RandomGraphGenerator otherGenerator = proc.initializeGraphGenerator(10, 5, cfg);

        assertGraphEquals(generator.generate(), otherGenerator.generate());
    }

    static Stream<Arguments> relationshipPropertyProducers() {
        Collection<Arguments> producers = new ArrayList<>();

        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put(RELATIONSHIP_PROPERTY_NAME_KEY, "fixed");
        paramsMap.put(RELATIONSHIP_PROPERTY_TYPE_KEY, "FIXED");
        paramsMap.put(RELATIONSHIP_PROPERTY_VALUE_KEY, 42.0D);
        producers.add(Arguments.of(
            paramsMap,
            new PropertyProducer.FixedDoubleProducer("fixed", 42)
        ));

        paramsMap = new HashMap<>();
        paramsMap.put(RELATIONSHIP_PROPERTY_NAME_KEY, "random");
        paramsMap.put(RELATIONSHIP_PROPERTY_TYPE_KEY, "RANDOM");
        paramsMap.put(RELATIONSHIP_PROPERTY_MIN_KEY, 21.0D);
        paramsMap.put(RELATIONSHIP_PROPERTY_MAX_KEY, 42.0D);
        producers.add(Arguments.of(
            paramsMap,
            new PropertyProducer.RandomDoubleProducer("random", 21, 42)
        ));

        paramsMap = new HashMap<>();
        paramsMap.put(RELATIONSHIP_PROPERTY_NAME_KEY, "random");
        paramsMap.put(RELATIONSHIP_PROPERTY_TYPE_KEY, "RANDOM");

        producers.add(Arguments.of(
            paramsMap,
            new PropertyProducer.RandomDoubleProducer("random", 0, 1)
        ));

        return producers.stream();
    }

    static Stream<Arguments> invalidRelationshipPropertyProducers() {
        Collection<Arguments> producers = new ArrayList<>();

        producers.add(Arguments.of(
            "Missing `name`",
            map(
                "foobar", "baz"
            ),
            asList("`name`", "specified")
        ));

        producers.add(Arguments.of(
            "Missing `type`",
            map(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop"
            ),
            asList("`type`", "specified")
        ));

        producers.add(Arguments.of(
            "Invalid type for `type`",
            map(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "foobar"
            ),
            asList("Unknown Relationship property generator", "foobar")
        ));

        producers.add(Arguments.of(
            "Invalid type for `min`",
            map(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "RANDOM",
                RELATIONSHIP_PROPERTY_MIN_KEY, "Zweiundvierzig"
            ),
            asList(RELATIONSHIP_PROPERTY_MIN_KEY, "of type `Double`", "`String`")
        ));

        producers.add(Arguments.of(
            "Invalid type for `max`",
            map(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "RANDOM",
                RELATIONSHIP_PROPERTY_MIN_KEY, 0.0,
                RELATIONSHIP_PROPERTY_MAX_KEY, "Zweiundvierzig"
            ),
            asList(RELATIONSHIP_PROPERTY_MAX_KEY, "of type `Double`", "`String`")
        ));

        producers.add(Arguments.of(
            "Invalid type for `value`",
            map(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "FIXED",
                RELATIONSHIP_PROPERTY_VALUE_KEY, "Zweiundvierzig"
            ),
            asList(RELATIONSHIP_PROPERTY_VALUE_KEY, "of type `Double`", "`String`")
        ));

        producers.add(Arguments.of(
            "Null value for `value`",
            map(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "FIXED",
                RELATIONSHIP_PROPERTY_VALUE_KEY, null
            ),
            asList(RELATIONSHIP_PROPERTY_VALUE_KEY, "No value specified")
        ));

        return producers.stream();
    }

    private static Stream<Arguments> estimations() {
        return Stream.of(
            Arguments.of(100, 2, MemoryRange.of(28_088, 31_288)),
            Arguments.of(100, 4, MemoryRange.of(29_688, 34_488)),
            Arguments.of(200, 4, MemoryRange.of(59_304, 68_904))
        );
    }
}
