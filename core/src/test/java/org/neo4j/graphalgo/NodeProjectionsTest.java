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
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
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
        assertThat(actual.labelProjection(), equalTo(Optional.of("A")));
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
        assertThat(actual.labelProjection(), equalTo(Optional.of("A")));
    }

    static Stream<Object> multipleNodeLabels() {
        return Stream.of("A | B", Arrays.asList("A", "B"));
    }

    @ParameterizedTest
    @MethodSource("multipleNodeLabels")
    void shouldParseMultipleLabels(Object input) {
        NodeProjections actual = NodeProjections.fromObject(input);

        NodeProjections expected = NodeProjections.builder()
            .putProjection(ElementIdentifier.of("A"), NodeProjection.builder().label("A").build())
            .putProjection(ElementIdentifier.of("B"), NodeProjection.builder().label("B").build())
            .build();

        assertThat(actual, equalTo(expected));
        assertThat(actual.labelProjection().get(), equalTo("A|B"));
    }

    @Test
    void shouldSupportStar() {
        NodeProjections actual = NodeProjections.fromObject("*");

        NodeProjections expected = NodeProjections.builder().projections(singletonMap(
            PROJECT_ALL,
            NodeProjection
                .builder()
                .label((String) null)
                .properties(PropertyMappings.of())
                .build()
        )).build();

        assertThat(
            actual,
            equalTo(expected)
        );
        assertThat(actual.labelProjection(), equalTo(Optional.of("")));
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