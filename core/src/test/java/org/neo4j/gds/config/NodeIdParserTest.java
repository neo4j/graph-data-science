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
package org.neo4j.gds.config;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.core.NodeEntity;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeIdParserTest {

    private static final List<Number> LIST_OF_NUMBERS = List.of(
        (byte) 1,
        (short) 1,
        (int) 1,
        (long) 1,
        (float) 1,
        (double) 1
    );

    @Test
    void shouldParseSingleNodeIdFromSingleNumber() {
        for (Number number : LIST_OF_NUMBERS) {
            assertThat(NodeIdParser.parseToSingleNodeId(number, "")).isEqualTo(1L);
        }
    }

    @Test
    void shouldParseSingleNodeIdFromSingletonListOfNumber() {
        for (Number number : LIST_OF_NUMBERS) {
            assertThat(NodeIdParser.parseToSingleNodeId(List.of(number), "")).isEqualTo(1L);
        }
    }

    @Test
    void shouldParseSingleNodeIdFromNode() {
        var node = new NodeEntity(null, 1337L);
        assertThat(NodeIdParser.parseToSingleNodeId(node, "")).isEqualTo(1337L);
    }

    @Test
    void shouldFailWithMessageParsingSingleNodeIdFromNonSingletonList() {
        var inputWithTooManyElements = List.of(1L, 2L);
        assertThatThrownBy(() -> NodeIdParser.parseToSingleNodeId(inputWithTooManyElements, "foobar"))
            .hasMessageContaining("Failed to parse `foobar` as a single node ID")
            .hasMessageContaining("this `List12` contains `2` elements.")
            .isInstanceOf(IllegalArgumentException.class);
        var inputWithNoElements = Set.of();
        assertThatThrownBy(() -> NodeIdParser.parseToSingleNodeId(inputWithNoElements, "sourceNodes"))
            .hasMessageContaining("Failed to parse `sourceNodes` as a single node ID")
            .hasMessageContaining("this `SetN` contains `0` elements.")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldParseListOfNodeIdFromNumber() {
        for (Number number : LIST_OF_NUMBERS) {
            assertThat(NodeIdParser.parseToListOfNodeIds(number, "")).isEqualTo(List.of(1L));
        }
        var actual = NodeIdParser.parseToListOfNodeIds(LIST_OF_NUMBERS, "");
        assertThat(actual).allMatch(l -> l == 1L);
    }

    @Test
    void shouldParseListOfNodeIdFromListOfNumbers() {
        var actual = NodeIdParser.parseToListOfNodeIds(LIST_OF_NUMBERS, "");
        assertThat(actual).allMatch(l -> l == 1L);
    }

    @Test
    void shouldParseListOfNodeIdFromNode() {
        var node = new NodeEntity(null, 1337L);
        assertThat(NodeIdParser.parseToListOfNodeIds(node, "")).isEqualTo(List.of(1337L));
    }

    @Test
    void shouldParseListOfNodeIdFromListOfNodesAndNumbers() {
        var input = List.of(
            new NodeEntity(null, 1),
            (double) 1,
            new NodeEntity(null, 2),
            (long) 2
        );
        assertThat(NodeIdParser.parseToListOfNodeIds(input, "")).isEqualTo(List.of(1L, 1L, 2L, 2L));
    }

    @Test
    void shouldFailWithMessageParsingListOfNodeIdFromCollectionContainingInvalidElements() {
        var input = List.of(
            1,
            "2"
        );
        assertThatThrownBy(() -> NodeIdParser.parseToListOfNodeIds(input, "testParam"))
            .hasMessageContaining("Failed to parse `testParam` as a List of node IDs.")
            .hasMessageContaining("this `String` cannot")
            .isInstanceOf(IllegalArgumentException.class);
    }
}
