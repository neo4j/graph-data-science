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

import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryTree;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

public final class MemoryEstimateResultFactory {

    private MemoryEstimateResultFactory() {}

    public static MemoryEstimateResult from(MemoryTreeWithDimensions memoryWithDimensions) {
        return from(memoryWithDimensions.memoryTree, memoryWithDimensions.graphDimensions);
    }

    public static MemoryEstimateResult from(MemoryTree memory, GraphDimensions dimensions) {
        return from(memory.render(), memory.renderMap(), memory.memoryUsage(), dimensions.nodeCount(), dimensions.relCountUpperBound());
    }

    public static MemoryEstimateResult from(
        String treeView,
        Map<String, Object> mapView,
        MemoryRange estimateMemoryUsage,
        long nodeCount,
        long relationshipCount
    ) {
        // FIXME: pass the heap size from the outside?
        var heapSize = Runtime.getRuntime().maxMemory();

        return new MemoryEstimateResult(
            estimateMemoryUsage.toString(),
            treeView,
            mapView,
            estimateMemoryUsage.min,
            estimateMemoryUsage.max,
            nodeCount,
            relationshipCount,
            getPercentage(estimateMemoryUsage.min, heapSize),
            getPercentage(estimateMemoryUsage.max, heapSize)
        );
    }

    private static double getPercentage(long requiredBytes, long heapSizeBytes) {
        if (heapSizeBytes == 0) {
            return Double.NaN;
        }
        return BigDecimal.valueOf(requiredBytes)
            .divide(BigDecimal.valueOf(heapSizeBytes), 1, RoundingMode.UP)
            .doubleValue();
    }
}
