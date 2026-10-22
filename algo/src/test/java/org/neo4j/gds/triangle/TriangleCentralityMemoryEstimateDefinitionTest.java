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
package org.neo4j.gds.triangle;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.concurrency.Concurrency;

import static org.neo4j.gds.assertions.MemoryEstimationAssert.assertThat;

class TriangleCentralityMemoryEstimateDefinitionTest {

    // Cover empty arrays, the 128-to-129-bit word boundary, and node counts requiring paged arrays.
    // Concurrency changes the number of source-edge batches, while the graph-sized arrays stay shared.
    @ParameterizedTest
    @CsvSource({
        "0, 0, 1, 12816",
        "1, 0, 1, 12872",
        "10, 18, 1, 13376",
        "100, 128, 1, 18424",
        "100, 129, 1, 18432",
        "100, 200, 4, 55472",
        "10000, 0, 4, 609848",
        "1000000000, 0, 1, 56008557912",
        "100000000000, 0, 1, 5600854505112"
    })
    void memoryEstimation(long nodeCount, long relationshipCount, int concurrency, long expectedBytes) {
        // Projected relationship counts already include both directions of undirected relationships.
        var memoryEstimation = new TriangleCentralityMemoryEstimateDefinition().memoryEstimation();

        assertThat(memoryEstimation)
            .memoryRange(nodeCount, relationshipCount, new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expectedBytes);
    }
}
