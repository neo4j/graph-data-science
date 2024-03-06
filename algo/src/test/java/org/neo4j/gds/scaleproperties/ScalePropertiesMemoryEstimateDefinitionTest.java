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
package org.neo4j.gds.scaleproperties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ScalePropertiesMemoryEstimateDefinitionTest {

    @Test
    void shouldEstimateMemoryFromGraph() {

        var gdlGraph = GdlFactory.of("(a:A {a: 1.1D, b: 20, c: 50, bAndC: [20.0, 50.0], longArrayB: [20L], floatArrayB: [20.0], doubleArray: [1.000000001d],  mixedSizeArray: [1.0, 1.0], missingArray: [1.0,2.0]})," +
            "(b:A {a: 2.8D, b: 21, c: 51, bAndC: [21.0, 51.0], longArrayB: [21L], floatArrayB: [21.0], doubleArray: [1.000000002d], mixedSizeArray: [1.0]}), "+
            "(c:A {a: 3, b: 22, c: 52, bAndC: [22.0, 52.0], longArrayB: [22L], floatArrayB: [22.0], doubleArray: [1.000000003d], mixedSizeArray: [1.0]}), " +
            "(d:A {a: -1, b: 23, c: 60, bAndC: [23.0, 60.0], longArrayB: [23L], floatArrayB: [23.0], doubleArray: [1.000000004d], mixedSizeArray: [1.0]}), " +
            "(e:A {a: -10, b: 24, c: 100, bAndC: [24.0, 100.0], longArrayB: [24L], floatArrayB: [24.0], doubleArray: [1.000000005d], mixedSizeArray: [1.0, 2.0]})");

        var config= mock(ScalePropertiesBaseConfig.class);
        when(config.nodeProperties()).thenReturn(List.of("bAndC", "longArrayB"));

        var memoryEstimation = new ScalePropertiesMemoryEstimateDefinition(config)
            .memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(gdlGraph.dimensions(),1)
            .hasSameMinAndMaxEqualTo(288);
    }

    @ParameterizedTest
    @CsvSource(value = {
        // BASE
        "   1_000, 1_044_064",

        // Should increase linearly with node count
        "   2_000, 2_088_064"
    })
    void shouldEstimateMemoryFromCounts(
        long nodeCount,
        long expectedMemory
    ) {

        var config= mock(ScalePropertiesBaseConfig.class);
        when(config.nodeProperties()).thenReturn(List.of("DUMMY"));

        var memoryEstimation = new ScalePropertiesMemoryEstimateDefinition(config)
            .memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(nodeCount,1)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

}
