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
package org.neo4j.gds.applications.graphstorecatalog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.assertGraphNotEquals;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_MAX_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_MIN_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_NAME_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_TYPE_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_VALUE_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GenerateGraphApplicationTest {
    /**
     * This does not test much at all, but we had this for the procedure, so keeping it here
     */
    @ParameterizedTest
    @EnumSource(RelationshipDistribution.class)
    void shouldParseGraphGeneratorConfiguration(RelationshipDistribution relationshipDistribution) {
        var generator = GenerateGraphApplication.initializeGraphGenerator(
            RandomGraphGeneratorConfig.of(
                "dumb",
                "dumber",
                10,
                5,
                CypherMapWrapper.create(Map.of(
                    RandomGraphGeneratorConfig.RELATIONSHIP_DISTRIBUTION_KEY,
                    relationshipDistribution.name()
                ))
            )
        );

        assertEquals(relationshipDistribution, generator.getRelationshipDistribution());
        assertFalse(generator.getMaybeRelationshipPropertyProducer().isPresent());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRelationshipPropertyProducers")
    void shouldThrowOnInvalidRelationshipPropertyParameters(
        String description,
        Object config,
        Iterable<String> errorFragments
    ) {
        assertThatIllegalArgumentException()
            .isThrownBy(() ->
                GenerateGraphApplication.initializeGraphGenerator(
                    RandomGraphGeneratorConfig.of(
                        "dumb",
                        "dumber",
                        10,
                        5,
                        CypherMapWrapper.create(Map.of(
                                RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_KEY,
                                config
                            )
                        )
                    )
                )
            )
            .extracting(IllegalArgumentException::getMessage)
            .satisfies(errorMessage -> errorFragments.forEach((expectedErrorFragment) -> assertTrue(
                errorMessage.contains(expectedErrorFragment),
                formatWithLocale(
                    "Expected error message to contain `%s`, but got `%s`",
                    expectedErrorFragment,
                    errorMessage
                )
            )));
    }

    @Test
    void shouldGenerateGraphFromDefaults() {
        var generator = GenerateGraphApplication.initializeGraphGenerator(
            RandomGraphGeneratorConfig.of(
                "dumb",
                "dumber",
                10,
                5,
                CypherMapWrapper.empty()
            )
        );

        assertEquals(generator.getRelationshipDistribution(), RelationshipDistribution.UNIFORM);
        assertFalse(generator.getMaybeRelationshipPropertyProducer().isPresent());
    }

    @Test
    void shouldAllowSeeding() {
        var generator1 = getGraphGenerator(42);
        var generator2 = getGraphGenerator(42);
        var generator3 = getGraphGenerator(87);

        assertGraphEquals(generator1.generate(), generator2.generate());
        assertGraphNotEquals(generator1.generate(), generator3.generate());
    }

    private static RandomGraphGenerator getGraphGenerator(long randomSeed) {
        return GenerateGraphApplication.initializeGraphGenerator(
            RandomGraphGeneratorConfig.of(
                "dumb",
                "dumber",
                10,
                5,
                CypherMapWrapper.create(Map.of("relationshipSeed", randomSeed))
            )
        );
    }

    static Stream<Arguments> invalidRelationshipPropertyProducers() {
        Collection<Arguments> producers = new ArrayList<>();

        producers.add(Arguments.of(
            "Missing `name`",
            Map.of(
                "foobar", "baz"
            ),
            asList("`name`", "specified")
        ));

        producers.add(Arguments.of(
            "Missing `type`",
            Map.of(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop"
            ),
            asList("`type`", "specified")
        ));

        producers.add(Arguments.of(
            "Invalid type for `type`",
            Map.of(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "foobar"
            ),
            asList("Unknown Relationship property generator", "foobar")
        ));

        producers.add(Arguments.of(
            "Invalid type for `min`",
            Map.of(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "RANDOM",
                RELATIONSHIP_PROPERTY_MIN_KEY, "Zweiundvierzig"
            ),
            asList(RELATIONSHIP_PROPERTY_MIN_KEY, "of type `Double`", "`String`")
        ));

        producers.add(Arguments.of(
            "Invalid type for `max`",
            Map.of(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "RANDOM",
                RELATIONSHIP_PROPERTY_MIN_KEY, 0.0,
                RELATIONSHIP_PROPERTY_MAX_KEY, "Zweiundvierzig"
            ),
            asList(RELATIONSHIP_PROPERTY_MAX_KEY, "of type `Double`", "`String`")
        ));

        producers.add(Arguments.of(
            "Invalid type for `value`",
            Map.of(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "FIXED",
                RELATIONSHIP_PROPERTY_VALUE_KEY, "Zweiundvierzig"
            ),
            asList(RELATIONSHIP_PROPERTY_VALUE_KEY, "of type `Double`", "`String`")
        ));

        producers.add(Arguments.of(
            "Null value for `value`",
            mapWithNulls(
                RELATIONSHIP_PROPERTY_NAME_KEY, "prop",
                RELATIONSHIP_PROPERTY_TYPE_KEY, "FIXED",
                RELATIONSHIP_PROPERTY_VALUE_KEY, null
            ),
            asList(RELATIONSHIP_PROPERTY_VALUE_KEY, "No value specified")
        ));

        return producers.stream();
    }

    private static Map<String, Object> mapWithNulls(Object... objects) {
        var map = new HashMap<String, Object>();
        int i = 0;
        while (i < objects.length) {
            map.put((String) objects[i++], objects[i++]);
        }
        return map;
    }
}
