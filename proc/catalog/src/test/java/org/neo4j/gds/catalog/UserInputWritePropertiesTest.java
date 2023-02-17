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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.core.NodeEntity;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class UserInputWritePropertiesTest {

    @Test
    void shouldParseSingleString() {
        assertThat(UserInputWriteProperties.parse("string", "nodeProperties").stream().map(v -> v.writeProperty()).collect(
            Collectors.toList()))
            .isEqualTo(List.of("string"));
    }

    @Test
    void shouldParseListOfString() {
        assertThat(UserInputWriteProperties
            .parse(List.of("foo", "bar"), "nodeProperties")
            .stream()
            .map(v -> v.writeProperty())
            .collect(
                Collectors.toList()))
            .isEqualTo(List.of("foo", "bar"));
    }

    @Test
    void shouldParseMap() {
        assertThat(UserInputWriteProperties
            .parse(Map.of("a", "b", "c", "d"), "nodeProperties")
            .stream()
            .map(v -> v.writeProperty())
            .collect(
                Collectors.toList()))
            .containsExactlyInAnyOrder("b", "d");
    }

    @Test
    void shouldParseListOfMapsAndStrings() {
        assertThat(UserInputWriteProperties
            .parse(List.of("foo", Map.of("a", "b")), "nodeProperties")
            .stream()
            .map(v -> v.writeProperty())
            .collect(
                Collectors.toList()))
            .isEqualTo(List.of("foo", "b"));
        assertThat(UserInputWriteProperties
            .parse(List.of("foo", Map.of("a", "b")), "nodeProperties")
            .stream()
            .map(v -> v.nodeProperty())
            .collect(
                Collectors.toList()))
            .isEqualTo(List.of("foo", "a"));
    }


    static Stream<Arguments> typesInput() {
        return Stream.of(
            arguments(1, "number"),
            arguments(Boolean.TRUE, "boolean"),
            arguments(new NodeEntity(null, 1), "node"),
            arguments(Neo4jProxy.virtualRelationship(
                0,
                new NodeEntity(null, 1),
                new NodeEntity(null, 2),
                RelationshipType.withName("FOO")
            ), "relationship"),
            arguments(PathImpl.singular(new NodeEntity(null, 1)), "path")
        );
    }

    @ParameterizedTest
    @MethodSource("typesInput")
    void shouldNotParse(Object input, String type) {
        assertThatThrownBy(() -> UserInputWriteProperties.parse(input, "nodeProperties"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("but found %s", type);
    }

    static Stream<Arguments> mapInput() {
        return Stream.of(
            arguments(Map.of("leet", 42)),
            arguments(Map.of(42, "leet")),
            arguments(Map.of("42", "leet", 1, "foo")),
            arguments(Map.of("42", "leet", "foo", 1)),
            arguments(Map.of("42", "leet", "foo", List.of(1)))

        );

    }

    static Stream<Arguments> listInput() {
        return Stream.of(
            arguments(List.of("foo", Map.of("leet", 42))),
            arguments(List.of("foo", List.of("leet")))

        );
    }

    @ParameterizedTest
    @MethodSource("mapInput")
    void shouldNotParseImproperMap(Map map) {
        assertThatThrownBy(() -> UserInputWriteProperties.parse(map, "nodeProperties"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(" but found improperly defined map");
    }

    @ParameterizedTest
    @MethodSource("listInput")
    void shouldNotParseImproperList(List list) {
        assertThatThrownBy(() -> UserInputWriteProperties.parse(list, "nodeProperties"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(" but found improperly defined list");
    }

}
