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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.triangle.TriangleCentralityResult;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class TriangleCentralityResultBuilderForStreamMode implements StreamResultBuilder<TriangleCentralityResult, TriangleCentralityStreamResult> {
    @Override
    public Stream<TriangleCentralityStreamResult> build(
        NodeTranslator nodeTranslator,
        GraphStore graphStore,
        Optional<TriangleCentralityResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var algorithmResult = result.get();

        var centralities = algorithmResult.centralities();
        var triangleCounts = algorithmResult.triangleCounts();
        var triangleNeighbors = algorithmResult.triangleNeighbors();
        var allNeighbors = algorithmResult.allNeighbors();
        var triangleNeighborsTriangleSums = algorithmResult.triangleNeighborsTriangleSums();
        var allNeighborsTriangleSums = algorithmResult.allNeighborsTriangleSums();

        return LongStream.range(0, nodeTranslator.nodeCount())
            .mapToObj(nodeId -> {
                long triangleNeighborCount = triangleNeighbors.get(nodeId);
                long nonTriangleNeighborCount = allNeighbors.get(nodeId) - triangleNeighborCount;
                long triangleNeighborTriangleSum = triangleNeighborsTriangleSums.get(nodeId);
                long nonTriangleNeighborTriangleSum = allNeighborsTriangleSums.get(nodeId) - triangleNeighborTriangleSum;
                return new TriangleCentralityStreamResult(
                    nodeTranslator.toOriginalNodeId(nodeId),
                    centralities.get(nodeId),
                    triangleCounts.get(nodeId),
                    triangleNeighborCount,
                    nonTriangleNeighborCount,
                    triangleNeighborTriangleSum,
                    nonTriangleNeighborTriangleSum
                );
            });
    }
}
