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
package org.neo4j.gds.similarity.filtering;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeFilterSpecFactoryTest {

    @Test
    void shouldFailToParseInvalidInput() {
        var validInput = 1L;

        // double is invalid
        assertThatThrownBy(() -> NodeFilterSpecFactory.create(1.0))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid scalar type. Expected Long or Node but found: Double");
        assertThatThrownBy(() -> NodeFilterSpecFactory.create(List.of(validInput, 1.0)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid types in list. Expected Longs or Nodes but found [Double]");

        // String is valid as scalar but not in a list
        assertThatNoException().isThrownBy(() -> NodeFilterSpecFactory.create("foo"));
        assertThatThrownBy(() -> NodeFilterSpecFactory.create(List.of(validInput, "foo")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid types in list. Expected Longs or Nodes but found [String]");
    }
}
