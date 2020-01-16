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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.generator.RelationshipDistribution;
import org.neo4j.graphalgo.generator.RelationshipPropertyProducer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.GraphGenerateProc.RELATIONSHIP_SEED_KEY;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_DISTRIBUTION_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTIES_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_MAX_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_MIN_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_NAME_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_TYPE_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_VALUE_KEY;
import static org.neo4j.helpers.collection.MapUtil.map;

class GraphGenerateProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphGenerateProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldGenerateGraphWithDefaults() {
        String query = "CALL algo.beta.graph.generate(" +
                       "    'foo', 10, 5 " +
                       ")";

        runQueryWithRowConsumer(query,
                row -> {
                    assertEquals(10, row.getNumber("nodes").intValue());
                    assertEquals(50, row.getNumber("relationships").intValue());
                    assertEquals("foo", row.getString(RELATIONSHIP_PROPERTY_NAME_KEY));
                    assertEquals("UNIFORM", row.get(RELATIONSHIP_DISTRIBUTION_KEY));
                    assertNull(row.get(RELATIONSHIP_PROPERTY_KEY));
                    assertNull(row.get(RELATIONSHIP_SEED_KEY));
                }
        );
    }

    @ParameterizedTest
    @EnumSource(RelationshipDistribution.class)
    void shouldGenerateGraphWithRelationshipDistribution(RelationshipDistribution relationshipDistribution) {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RELATIONSHIP_DISTRIBUTION_KEY, relationshipDistribution.name());

        ProcedureConfiguration procedureConfig = ProcedureConfiguration.create(configMap, getUsername());

        GraphGenerateProc proc = new GraphGenerateProc();
        RandomGraphGenerator generator = proc.initializeGraphGenerator(10, 5, procedureConfig);

        assertEquals(relationshipDistribution, generator.getRelationshipDistribution());
        assertFalse(generator.getMaybePropertyProducer().isPresent());
    }

    @ParameterizedTest
    @MethodSource("relationshipPropertyProducers")
    void shouldGenerateGraphWithRelationshipProperty(
            Map<String, Object> config,
            RelationshipPropertyProducer propertyProducer) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RELATIONSHIP_PROPERTY_KEY, config);

        ProcedureConfiguration procedureConfig = ProcedureConfiguration.create(configMap, getUsername());

        GraphGenerateProc proc = new GraphGenerateProc();
        RandomGraphGenerator generator = proc.initializeGraphGenerator(10, 5, procedureConfig);

        assertTrue(generator.getMaybePropertyProducer().isPresent());
        RelationshipPropertyProducer actuallPropertyProducer = generator.getMaybePropertyProducer().get();

        assertEquals(propertyProducer, actuallPropertyProducer);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRelationshipPropertyProducers")
    void shouldThrowOnInvalidRelationshipPropertyParameters(
            String description,
            Object config,
            Iterable<String> errorFragments) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(RELATIONSHIP_PROPERTY_KEY, config);

        ProcedureConfiguration procedureConfig = ProcedureConfiguration.create(configMap, getUsername());

        GraphGenerateProc proc = new GraphGenerateProc();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> proc.initializeGraphGenerator(10, 5, procedureConfig)
        );

        String message = exception.getMessage();
        errorFragments.forEach((expectedErrorFragment) -> {
            assertTrue(
                    message.contains(expectedErrorFragment),
                    String.format(
                            "Expected error message to contain `%s`, but got `%s`",
                            expectedErrorFragment,
                            message));
        });
    }

    @Test
    void shouldGenerateGraphFromDefaults() {
        Map<String, Object> configMap = new HashMap<>();

        ProcedureConfiguration procedureConfig = ProcedureConfiguration.create(configMap, getUsername());

        GraphGenerateProc proc = new GraphGenerateProc();
        RandomGraphGenerator generator = proc.initializeGraphGenerator(10, 5, procedureConfig);

        assertEquals(generator.getRelationshipDistribution(), RelationshipDistribution.UNIFORM);
        assertFalse(generator.getMaybePropertyProducer().isPresent());
    }

    @Test
    void shouldBeSeedableGenerator() {
        long relationshipSeed = 4242L;

        Map<String, Object> configMap = map("relationshipSeed", relationshipSeed);

        ProcedureConfiguration procedureConfig = ProcedureConfiguration.create(configMap, getUsername());

        GraphGenerateProc proc = new GraphGenerateProc();
        RandomGraphGenerator generator = proc.initializeGraphGenerator(10, 5, procedureConfig);
        RandomGraphGenerator otherGenerator = proc.initializeGraphGenerator(10, 5, procedureConfig);

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
                new RelationshipPropertyProducer.Fixed("fixed", 42)
        ));

        paramsMap = new HashMap<>();
        paramsMap.put(RELATIONSHIP_PROPERTY_NAME_KEY, "random");
        paramsMap.put(RELATIONSHIP_PROPERTY_TYPE_KEY, "RANDOM");
        paramsMap.put(RELATIONSHIP_PROPERTY_MIN_KEY, 21.0D);
        paramsMap.put(RELATIONSHIP_PROPERTY_MAX_KEY, 42.0D);
        producers.add(Arguments.of(
                paramsMap,
                new RelationshipPropertyProducer.Random("random", 21, 42)
        ));

        paramsMap = new HashMap<>();
        paramsMap.put(RELATIONSHIP_PROPERTY_NAME_KEY, "random");
        paramsMap.put(RELATIONSHIP_PROPERTY_TYPE_KEY, "RANDOM");

        producers.add(Arguments.of(
                paramsMap,
                new RelationshipPropertyProducer.Random("random", 0, 1)
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

        producers.add(Arguments.of(
                "Invalid type for `relationshipProperty`",
                "some other config",
                asList(RELATIONSHIP_PROPERTIES_KEY, "Map", "String")
        ));

        return producers.stream();
    }
}
