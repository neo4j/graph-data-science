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
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.core.NodeEntity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserInputAsStringOrListOfStringTest {

    @Test
    void shouldParseSingleString() {
        assertThat(UserInputAsStringOrListOfString.parse("string"))
            .isEqualTo(List.of("string"));
    }

    @Test
    void shouldParseListOfString() {
        assertThat(UserInputAsStringOrListOfString.parse(List.of("foo", "bar")))
            .isEqualTo(List.of("foo", "bar"));
    }

    @Test
    void shouldNotParseNumber() {
        assertThatThrownBy(() -> UserInputAsStringOrListOfString.parse(1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Type mismatch for nodeProperties: expected List<String> or String, but found number");
    }

    @Test
    void shouldNotParseBoolean() {
        assertThatThrownBy(() -> UserInputAsStringOrListOfString.parse(Boolean.TRUE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Type mismatch for nodeProperties: expected List<String> or String, but found boolean");
    }

    @Test
    void shouldNotParseNode() {
        assertThatThrownBy(() -> UserInputAsStringOrListOfString.parse(new NodeEntity(null, 1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Type mismatch for nodeProperties: expected List<String> or String, but found node");
    }

    @Test
    void shouldNotParseRelationship() {
        var node1 = new NodeEntity(null, 0);
        var node2 = new NodeEntity(null, 1);
        var relationship = Neo4jProxy.virtualRelationship(0, node1, node2, RelationshipType.withName("FOO"));
        assertThatThrownBy(() -> UserInputAsStringOrListOfString.parse(relationship))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Type mismatch for nodeProperties: expected List<String> or String, but found relationship");
    }

    @Test
    void shouldNotParsePath() {
        var node = new NodeEntity(null, 0);
        var path = PathImpl.singular(node);
        assertThatThrownBy(() -> UserInputAsStringOrListOfString.parse(path))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Type mismatch for nodeProperties: expected List<String> or String, but found node");
    }

    @Test
    void shouldNotParseMap() {
        assertThatThrownBy(() -> UserInputAsStringOrListOfString.parse(Map.of("string", 1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Type mismatch for nodeProperties: expected List<String> or String, but found map");
    }

    @Test
    void shouldNotParseList() {
        assertThatThrownBy(() -> UserInputAsStringOrListOfString.parse(List.of(List.of(0))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Type mismatch for nodeProperties: expected List<String> or String, but found list");
    }

    @Test
    void shouldNotParseListWithNonStrings() {
        assertThatThrownBy(() -> UserInputAsStringOrListOfString.parse(List.of("string", 1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Type mismatch for nodeProperties: expected List<String> or String, but found number");
    }
}
