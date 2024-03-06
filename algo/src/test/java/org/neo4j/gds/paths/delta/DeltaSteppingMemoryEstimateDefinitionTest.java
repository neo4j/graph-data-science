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
package org.neo4j.gds.paths.delta;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.GraphDimensions;

class DeltaSteppingMemoryEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource({"10_000,100_000,260216,1280216", "100_000,1_000_000,2600216,12800216"})
    void memoryEstimation(long nodeCount, long relationshipCount, long expectedMin, long expectedMax) {
        var dimensions = GraphDimensions.builder().nodeCount(nodeCount).relCountUpperBound(relationshipCount).build();

        var memoryEstimation = new DeltaSteppingMemoryEstimateDefinition();

        MemoryEstimationAssert.assertThat(memoryEstimation.memoryEstimation())
            .memoryRange(dimensions,4)
            .hasMin(expectedMin)
            .hasMax(expectedMax);
    }

}
