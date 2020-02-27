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

import java.util.Arrays;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.AbstractNodeProjection.LABEL_KEY;
import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.ElementProjection.PROPERTIES_KEY;
import static org.neo4j.graphalgo.compat.MapUtil.map;

class NodeProjectionsTest {

    @ParameterizedTest(name = "argument: {0}")
    @MethodSource("syntacticSugarsSimple")
    void syntacticSugars(Object argument) {
        NodeProjections actual = NodeProjections.fromObject(argument);

        NodeProjections expected = NodeProjections.builder().projections(singletonMap(
            ElementIdentifier.of("A"),
            NodeProjection.builder().label("A").properties(PropertyMappings.of()).build()
        )).build();

        assertThat(
            actual,
            equalTo(expected)
        );
        assertThat(actual.labelProjection(), equalTo("A"));
    }

    @Test
    void shouldParseWithProperties() {
        NodeProjections actual = NodeProjections.fromObject(map(
            "MY_LABEL", map(
                "label", "A",
                "properties", asList(
                    "prop1", "prop2"
                )
            )
        ));

        NodeProjections expected = NodeProjections.builder().projections(singletonMap(
            ElementIdentifier.of("MY_LABEL"),
            NodeProjection
                .builder()
                .label("A")
                .properties(PropertyMappings
                    .builder()
                    .addMapping(PropertyMapping.of("prop1", Double.NaN))
                    .addMapping(PropertyMapping.of("prop2", Double.NaN))
                    .build()
                )
                .build()
        )).build();

        assertThat(
            actual,
            equalTo(expected)
        );
        assertThat(actual.labelProjection(), equalTo("A"));
    }

    @Test
    void shouldParseMultipleLabels() {
        NodeProjections actual = NodeProjections.fromObject(Arrays.asList("A", "B"));

        NodeProjections expected = NodeProjections.builder()
            .putProjection(ElementIdentifier.of("A"), NodeProjection.builder().label("A").build())
            .putProjection(ElementIdentifier.of("B"), NodeProjection.builder().label("B").build())
            .build();

        assertThat(actual, equalTo(expected));
        assertThat(actual.labelProjection(), equalTo("A|B"));
    }

    @Test
    void shouldSupportStar() {
        NodeProjections actual = NodeProjections.fromObject("*");

        NodeProjections expected = NodeProjections.builder().projections(singletonMap(
            PROJECT_ALL,
            NodeProjection
                .builder()
                .label("*")
                .properties(PropertyMappings.of())
                .build()
        )).build();

        assertThat(
            actual,
            equalTo(expected)
        );
        assertThat(actual.labelProjection(), equalTo(PROJECT_ALL.name));
    }

    @Test
    void shouldFailOnUnsupportedType() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> NodeProjections.fromObject(Arrays.asList("T", 42))
        );
        assertThat(ex.getMessage(), matchesPattern("Cannot construct a node projection out of a java.lang.Integer"));
    }

    static Stream<Arguments> syntacticSugarsSimple() {
        return Stream.of(
            Arguments.of(
                "A"
            ),
            Arguments.of(
                singletonList("A")
            ),
            Arguments.of(
                map("A", map(LABEL_KEY, "A"))
            ),
            Arguments.of(
                map("A", map(LABEL_KEY, "A", PROPERTIES_KEY, emptyMap()))
            )
        );
    }
}