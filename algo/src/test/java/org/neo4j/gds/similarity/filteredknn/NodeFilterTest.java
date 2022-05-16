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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.huge.DirectIdMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeFilterTest {

    @Test
    void shouldFailToParseInvalidInput() {
        var validInput = 1L;
        var idMap = new DirectIdMap(10);

        // double is invalid
        assertThatThrownBy(() -> NodeFilter.create(1.0, idMap))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid scalar type. Expected Long or Node but found: Double");
        assertThatThrownBy(() -> NodeFilter.create(List.of(validInput, 1.0), idMap))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid types in list. Expected Longs or Nodes but found [Double]");

        // String is valid as scalar but not in a list
//        assertThatNoException().isThrownBy(() -> NodeFilter.create("foo", idMap)); // Not implemented yet
        assertThatThrownBy(() -> NodeFilter.create(List.of(validInput, "foo"), idMap))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid types in list. Expected Longs or Nodes but found [String]");
    }

    @Test
    void shouldFilter() {
        var nodeFilter = NodeFilter.create(10L, new DirectIdMap(10));
        assertThat(nodeFilter.test(10)).isTrue();
        assertThat(nodeFilter.test(1)).isFalse();
    }

    @Test
    void noOpFilterShouldReturnTrue() {
        var nodeFilter = NodeFilter.noOp();
        assertThat(nodeFilter.test(10)).isTrue();
        assertThat(nodeFilter.test(1)).isTrue();
    }

}
