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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.core.Aggregation;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.AbstractRelationshipProjection.ORIENTATION_KEY;
import static org.neo4j.graphalgo.AbstractRelationshipProjection.TYPE_KEY;
import static org.neo4j.graphalgo.ElementProjection.PROPERTIES_KEY;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.core.Aggregation.SINGLE;

class RelationshipProjectionsTest {

    @Test
    void shouldParse() {
        Map<String, Object> noProperties = map(
            "MY_TYPE", map(
                "type", "T",
                "projection", "NATURAL",
                "aggregation", "SINGLE"
            ),
            "ANOTHER", map(
                "type", "FOO",
                "properties", Arrays.asList(
                    "prop1", "prop2"
                )
            )
        );

        RelationshipProjections projections = RelationshipProjections.fromObject(noProperties);
        assertThat(projections.allProjections(), hasSize(2));
        assertThat(
            projections.getFilter(ElementIdentifier.of("MY_TYPE")),
            equalTo(RelationshipProjection.of("T", Orientation.NATURAL, SINGLE))
        );
        assertThat(
            projections.getFilter(ElementIdentifier.of("ANOTHER")),
            equalTo(RelationshipProjection
                .builder()
                .type("FOO")
                .properties(PropertyMappings
                    .builder()
                    .addMapping(PropertyMapping.of("prop1", Double.NaN))
                    .addMapping(PropertyMapping.of("prop2", Double.NaN))
                    .build()
                )
                .build()
            )
        );
        assertThat(projections.typeFilter(), equalTo("T|FOO"));
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.RelationshipProjectionsTest#syntacticSugarsSimple")
    void syntacticSugars(Object argument) {
        RelationshipProjections actual = RelationshipProjections.fromObject(argument);

        RelationshipProjections expected = RelationshipProjections.builder().projections(singletonMap(
            ElementIdentifier.of("T"),
            RelationshipProjection
                .builder()
                .type("T")
                .orientation(Orientation.NATURAL)
                .aggregation(Aggregation.DEFAULT)
                .properties(PropertyMappings.of())
                .build()
        )).build();

        assertThat(actual, equalTo(expected));
        assertThat(actual.typeFilter(), equalTo("T"));
    }

    @Test
    void shouldSupportStar() {
        RelationshipProjections actual = RelationshipProjections.fromObject("*");

        RelationshipProjections expected = RelationshipProjections.builder()
            .projections(singletonMap(PROJECT_ALL, RelationshipProjection.all()))
            .build();

        assertThat(actual, equalTo(expected));
        assertThat(actual.typeFilter(), equalTo("*"));
    }

    @Test
    void shouldParseMultipleRelationshipTypes() {
        RelationshipProjections actual = RelationshipProjections.fromObject(Arrays.asList("A", "B"));

        RelationshipProjections expected = RelationshipProjections.builder()
            .putProjection(ElementIdentifier.of("A"), RelationshipProjection.builder().type("A").build())
            .putProjection(ElementIdentifier.of("B"), RelationshipProjection.builder().type("B").build())
            .build();

        assertThat(actual, equalTo(expected));
        assertThat(actual.typeFilter(), equalTo("A|B"));
    }

    @Test
    void shouldPropagateAggregationToProperty() {
        Map<String, Object> projection = map(
            "MY_TYPE", map(
                "type", "T",
                "projection", "NATURAL",
                "aggregation", "SINGLE",
                "properties", map(
                    "weight",
                    map("property", "weight")
                )
            )
        );

        RelationshipProjections actual = RelationshipProjections.fromObject(projection);

        RelationshipProjections expected = RelationshipProjections.builder().projections(
            singletonMap(
                ElementIdentifier.of("MY_TYPE"),
                RelationshipProjection
                    .builder()
                    .type("T")
                    .aggregation(SINGLE)
                    .properties(PropertyMappings.of(
                        PropertyMapping.of("weight", SINGLE)
                    ))
                    .build()
            )).build();

        assertThat(
            actual,
            equalTo(expected)
        );
    }

    @Test
    void shouldNotAllowCombiningStarWithStandard() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> RelationshipProjections.fromObject(Arrays.asList("*", "T"))
        );
        assertThat(ex.getMessage(), matchesPattern("A star projection .* cannot be combined.*"));
    }

    @Test
    void shouldFailOnUnsupportedType() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> RelationshipProjections.fromObject(Arrays.asList("T", 42))
        );
        assertThat(ex.getMessage(), matchesPattern("Cannot construct a relationship projection out of a java.lang.Integer"));
    }

    static Stream<Arguments> syntacticSugarsSimple() {
        return Stream.of(
            Arguments.of(
                "T"
            ),
            Arguments.of(
                singletonList("T")
            ),
            Arguments.of(
                map("T", map(TYPE_KEY, "T"))
            ),
            Arguments.of(
                map("T", map(TYPE_KEY, "T", ORIENTATION_KEY, Orientation.NATURAL.name()))
            ),
            Arguments.of(
                map("T", map(TYPE_KEY, "T", PROPERTIES_KEY, emptyMap()))
            ),
            Arguments.of(
                map(
                    "T",
                    map(TYPE_KEY, "T", ORIENTATION_KEY, Orientation.NATURAL.name(), PROPERTIES_KEY, emptyMap())
                )
            )
        );
    }
}
