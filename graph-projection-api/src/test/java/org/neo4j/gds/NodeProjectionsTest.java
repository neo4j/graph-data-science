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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.DefaultValue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.ElementProjection.PROPERTIES_KEY;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.NodeProjection.LABEL_KEY;

class NodeProjectionsTest {

    @ParameterizedTest(name = "argument: {0}")
    @MethodSource("syntacticSugarsSimple")
    void syntacticSugars(Object argument) {
        NodeProjections actual = NodeProjections.fromObject(argument);

        NodeProjections expected = NodeProjections.single(
            NodeLabel.of("A"),
            NodeProjection.builder().label("A").properties(PropertyMappings.of()).build()
        );

        assertThat(
            actual,
            equalTo(expected)
        );
        assertThat(actual.labelProjection(), equalTo("A"));
    }

    @Test
    void shouldParseWithProperties() {
        NodeProjections actual = NodeProjections.fromObject(Map.of(
            "MY_LABEL", Map.of(
                "label", "A",
                "properties", asList(
                    "prop1", "prop2"
                )
            )
        ));

        NodeProjections expected = NodeProjections.single(
            NodeLabel.of("MY_LABEL"),
            NodeProjection
                .builder()
                .label("A")
                .addProperties(
                    PropertyMapping.of("prop1", DefaultValue.DEFAULT),
                    PropertyMapping.of("prop2", DefaultValue.DEFAULT)
                )
                .build()
        );

        assertThat(
            actual,
            equalTo(expected)
        );
        assertThat(actual.labelProjection(), equalTo("A"));
    }

    @Test
    void shouldParseMultipleLabels() {
        NodeProjections actual = NodeProjections.fromObject(Arrays.asList("A", "B"));

        NodeProjections expected = NodeProjections.create(Map.of(
            NodeLabel.of("A"), NodeProjection.builder().label("A").build(),
            NodeLabel.of("B"), NodeProjection.builder().label("B").build()
        ));

        assertThat(actual, equalTo(expected));
        assertThat(actual.labelProjection(), equalTo("A, B"));
    }

    @Test
    void shouldSupportStar() {
        NodeProjections actual = NodeProjections.fromObject("*");

        NodeProjections expected = NodeProjections.single(
            ALL_NODES,
            NodeProjection
                .builder()
                .label("*")
                .properties(PropertyMappings.of())
                .build()
        );

        assertThat(
            actual,
            equalTo(expected)
        );
    }

    @Test
    void shouldFailOnUnsupportedType() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> NodeProjections.fromObject(Arrays.asList("T", 42))
        );
        assertThat(ex.getMessage(), matchesPattern("Cannot construct a node projection out of a java.lang.Integer"));
    }

    @Test
    void shouldFailOnDuplicatePropertyKeys() {
        assertThatThrownBy(() -> NodeProjection
            .builder()
            .label("Foo")
            .addAllProperties(PropertyMappings.fromObject(List.of("prop", "prop")))
            .build()
        )
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Duplicate property key `prop`");
    }

    static Stream<Arguments> ambiguousPropertyMappings() {
        return Stream.of(
            Arguments.of(
                "different neo key",
                PropertyMapping.of("foo", "bar", DefaultValue.DEFAULT),
                PropertyMapping.of("foo", "baz", DefaultValue.DEFAULT),
                new String[]{
                    "Specifying multiple neoPropertyKeys for the same property is not allowed, found propertyKey: `foo` with conflicting neoPropertyKeys:",
                    "`bar`",
                    "`baz`"
                }
            ),
            Arguments.of(
                "different default value types",
                PropertyMapping.of("foo", "baz", DefaultValue.forLong()),
                PropertyMapping.of("foo", "baz", DefaultValue.forDouble()),
                new String[]{
                    "Specifying different default values for the same property with identical neoPropertyKey is not allowed, found propertyKey: `foo` with conflicting default values:",
                    "`-9223372036854775808`",
                    "`NaN`",
                }
            ),
            Arguments.of(
                "different default values",
                PropertyMapping.of("foo", "baz", DefaultValue.of(42)),
                PropertyMapping.of("foo", "baz", DefaultValue.of(1337)),
                new String[]{
                    "Specifying different default values for the same property with identical neoPropertyKey is not allowed, found propertyKey: `foo` with conflicting default values:",
                    "`42`",
                    "`1337`",
                }
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ambiguousPropertyMappings")
    void shouldFailOnAmbiguousNodePropertyDefinition(
        String ignore,
        PropertyMapping first,
        PropertyMapping second,
        String... messages
    ) {
        var builder = ImmutableNodeProjections.builder().projections(Map.of(
            NodeLabel.of("A"),
            NodeProjection
                .builder()
                .label("A")
                .addProperty(first)
                .build(),
            NodeLabel.of("B"),
            NodeProjection
                .builder()
                .label("B")
                .addProperty(second)
                .build()
        ));

        var throwableAssert = assertThatThrownBy(builder::build)
            .isInstanceOf(IllegalArgumentException.class);
        for (var message : messages) {
            throwableAssert.hasMessageContaining(message);
        }
    }

    @Test
    void shouldSupportCaseInsensitiveConfigKeys() {
        NodeProjections actual = NodeProjections.fromObject(Map.of(
            "MY_LABEL", Map.of(
                "LABEL", "A",
                "PrOpErTiEs", asList(
                    "prop1", "prop2"
                )
            )
        ));

        NodeProjections expected = NodeProjections.single(
            NodeLabel.of("MY_LABEL"),
            NodeProjection
                .builder()
                .label("A")
                .addProperties(
                    PropertyMapping.of("prop1", DefaultValue.DEFAULT),
                    PropertyMapping.of("prop2", DefaultValue.DEFAULT)
                )
                .build()
        );

        assertThat(
            actual,
            equalTo(expected)
        );
        assertThat(actual.labelProjection(), equalTo("A"));
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
                Map.of("A", Map.of(LABEL_KEY, "A"))
            ),
            Arguments.of(
                Map.of("A", Map.of(LABEL_KEY, "A", PROPERTIES_KEY, emptyMap()))
            )
        );
    }
}
