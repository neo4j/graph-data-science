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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RandomGraphGeneratorConfigTest {

    @Test
    void testAverageDegreeLimit() {
        var configBuilder = RandomGraphGeneratorConfigImpl.builder().graphName("g").username("neo4j").nodeCount(1);

        assertThatThrownBy(() -> configBuilder.averageDegree(1).build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Cannot create any relationships if self-loops are not allowed and node-count is `1`.");

        configBuilder.allowSelfLoops(true);
        configBuilder.averageDegree(2).build();
    }
}
