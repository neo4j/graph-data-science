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

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;

public class TriangleCentralityMemoryEstimateDefinition implements MemoryEstimateDefinition {

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations
            .builder(TriangleCentrality.class)
            .add(
                "adjacency-entry-index",
                MemoryEstimations
                    .builder(AdjacencyEntryIndex.class)
                    .perNode("offsets", nodeCount -> HugeLongArray.memoryEstimation(nodeCount + 1))
                    .build()
            )
            .perGraphDimension(
                "triangle-edges",
                (dim, concurrency) -> MemoryRange.of(
                    HugeAtomicBitSet.memoryEstimation(dim.relCountUpperBound())
                )
            )
            .perThread("source-edge-batch", TriangleCentrality.sourceEdgeBatchMemoryEstimation())
            .perNode("centralities", HugeDoubleArray::memoryEstimation)
            .perNode("triangle-counts", HugeAtomicLongArray::memoryEstimation)
            .perNode("triangle-neighbors", HugeAtomicLongArray::memoryEstimation)
            .perNode("all-neighbors", HugeAtomicLongArray::memoryEstimation)
            .perNode("triangle-neighbors-triangle-sums", HugeAtomicLongArray::memoryEstimation)
            .perNode("all-neighbors-triangle-sums", HugeAtomicLongArray::memoryEstimation)
            .build();
    }
}
