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
package org.neo4j.gds.procedures.community;

import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountStreamResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class TriangleCountComputationResultTransformer {

    private TriangleCountComputationResultTransformer() {}

    static Stream<TriangleCountStreamResult> toStreamResult(StreamComputationResult<TriangleCountBaseConfig, TriangleCountResult> computationResult) {
        return computationResult.result().map(triangleCountResult -> {
            var graph = computationResult.graph();
            var result = computationResult.result().get();
            return LongStream.range(0, graph.nodeCount())
                .mapToObj(i -> new TriangleCountStreamResult(
                    graph.toOriginalNodeId(i),
                    result.localTriangles().get(i)
                ));

        }).orElseGet(Stream::empty);
    }


}
