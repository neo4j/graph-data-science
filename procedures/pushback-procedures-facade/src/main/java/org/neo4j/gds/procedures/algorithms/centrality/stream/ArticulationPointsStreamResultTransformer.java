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
package org.neo4j.gds.procedures.algorithms.centrality.stream;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.articulationpoints.SubtreeTracker;
import org.neo4j.gds.procedures.algorithms.centrality.ArticulationPointStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class ArticulationPointsStreamResultTransformer implements ResultTransformer<TimedAlgorithmResult<ArticulationPointsResult>, Stream<ArticulationPointStreamResult>> {
    private final Graph graph;

    public ArticulationPointsStreamResultTransformer(Graph graph) {
        this.graph = graph;
    }

    @Override
    public Stream<ArticulationPointStreamResult> apply(TimedAlgorithmResult<ArticulationPointsResult> timedAlgorithmResult) {
        var result = timedAlgorithmResult.result();
        var articulationPoints = result.articulationPoints();
        var subtreeTracker = result.subtreeTracker();

        var nodeCount = graph.nodeCount();
        return LongStream.range(0, nodeCount)
            .filter(articulationPoints::get)
            .mapToObj( v -> createResult(v, graph.toOriginalNodeId(v),subtreeTracker));
    }

    private  ArticulationPointStreamResult createResult(long node, long originalId, Optional<SubtreeTracker> subtreeTracker){
        return  subtreeTracker.map(
            tracker -> {
                var resultMap = Map.of("max",tracker.maxComponentSize(node),
                    "min", tracker.minComponentSize(node),
                    "count", tracker.remainingComponents(node)
                );
                return new ArticulationPointStreamResult(originalId,resultMap);
            }
        ).orElse(new ArticulationPointStreamResult(originalId,null));
    }
}
