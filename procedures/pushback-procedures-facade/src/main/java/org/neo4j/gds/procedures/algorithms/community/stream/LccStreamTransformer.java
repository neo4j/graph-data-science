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
package org.neo4j.gds.procedures.algorithms.community.stream;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.procedures.algorithms.community.LocalClusteringCoefficientStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class LccStreamTransformer implements ResultTransformer<TimedAlgorithmResult<LocalClusteringCoefficientResult>, Stream<LocalClusteringCoefficientStreamResult>> {

    private final Graph graph;

    public LccStreamTransformer(Graph graph) {this.graph = graph;}

    @Override
    public Stream<LocalClusteringCoefficientStreamResult> apply(
        TimedAlgorithmResult<LocalClusteringCoefficientResult> timedAlgorithmResult
    ) {
        var localClusteringCoefficientResult = timedAlgorithmResult.result();
        if (localClusteringCoefficientResult == LocalClusteringCoefficientResult.EMPTY) return Stream.empty();

        var localClusteringCoefficients = localClusteringCoefficientResult.localClusteringCoefficients();

        return LongStream.range(0, graph.nodeCount())
            .mapToObj(i -> new LocalClusteringCoefficientStreamResult(
                graph.toOriginalNodeId(i),
                localClusteringCoefficients.get(i)
            ));
    }
}
