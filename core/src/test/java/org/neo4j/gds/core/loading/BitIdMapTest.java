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
package org.neo4j.gds.core.loading;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BitIdMapTest {

    @ParameterizedTest
    @CsvSource({
        "0,168,208,248",
        "100,176,216,256",
        "100000000000,12988281408,12988281448,12988281488"
    })
    void shouldComputeMemoryEstimation(
        long highestPossibleNodeCount,
        long expectedBytes,
        long expectedBytesWithOneToken,
        long expectedBytesWithTwoTokens
    ) {
        GraphDimensions dimensions = ImmutableGraphDimensions
            .builder()
            .nodeCount(0)
            .highestPossibleNodeCount(highestPossibleNodeCount)
            .build();
        MemoryTree memRec = BitIdMap.memoryEstimation().estimate(dimensions, 1);
        assertEquals(MemoryRange.of(expectedBytes), memRec.memoryUsage());

        IntObjectMap<List<NodeLabel>> labelTokenNodeLabelMappings = new IntObjectHashMap<List<NodeLabel>>();
        labelTokenNodeLabelMappings.put(1, singletonList(NodeLabel.of("A")));

        dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(0)
            .highestPossibleNodeCount(highestPossibleNodeCount)
            .tokenNodeLabelMapping(labelTokenNodeLabelMappings)
            .build();
        memRec = BitIdMap.memoryEstimation().estimate(dimensions, 1);
        assertEquals(MemoryRange.of(expectedBytesWithOneToken), memRec.memoryUsage());

        labelTokenNodeLabelMappings.put(2, Arrays.asList(NodeLabel.of("A"), NodeLabel.of("B")));
        dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(0)
            .highestPossibleNodeCount(highestPossibleNodeCount)
            .tokenNodeLabelMapping(labelTokenNodeLabelMappings)
            .build();
        memRec = BitIdMap.memoryEstimation().estimate(dimensions, 1);
        assertEquals(MemoryRange.of(expectedBytesWithTwoTokens), memRec.memoryUsage());
    }
}
