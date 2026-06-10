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
package org.neo4j.gds.applications.algorithms.machinery;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.mem.MemoryRange;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MemoryEstimateResultFactoryTest {

    @Test
    void buildsFromAByteRangeWithExplicitCounts() {
        var range = MemoryRange.of(130, 270);

        var result = MemoryEstimateResultFactory.from("tree", Map.of("k", "v"), range, 1000, 5000);

        assertThat(result.bytesMin()).isEqualTo(130);
        assertThat(result.bytesMax()).isEqualTo(270);
        assertThat(result.nodeCount()).isEqualTo(1000);
        assertThat(result.relationshipCount()).isEqualTo(5000);
        assertThat(result.treeView()).isEqualTo("tree");
        assertThat(result.mapView()).isEqualTo(Map.of("k", "v"));
        assertThat(result.requiredMemory()).isEqualTo(range.toString());
    }

    @Test
    void recomputesHeapPercentageFromTheRange() {
        var zero = MemoryEstimateResultFactory.from("tree", Map.of(), MemoryRange.of(0, 0), 1, 1);
        assertThat(zero.heapPercentageMin()).isEqualTo(0.0);
        assertThat(zero.heapPercentageMax()).isEqualTo(0.0);

        var nonZeroMax = MemoryEstimateResultFactory.from("tree", Map.of(), MemoryRange.of(0, 1), 1, 1);
        assertThat(nonZeroMax.heapPercentageMin()).isEqualTo(0.0);
        assertThat(nonZeroMax.heapPercentageMax()).isGreaterThan(0.0);
    }
}
