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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.articulationpoints.SubtreeTracker;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class ArticulationPointsResultBuilderForStreamMode implements StreamResultBuilder<ArticulationPointsResult, ArticulationPointStreamResult> {

    @Override
    public Stream<ArticulationPointStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<ArticulationPointsResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var articulationPoints = result.get().articulationPoints();
        var subtreeTracker = result.get().subtreeTracker();

        var nodeCount = graph.nodeCount();
        return LongStream.range(0, nodeCount)
            .filter(articulationPoints::get)
            .mapToObj( v -> createResult(v, graph.toOriginalNodeId(v),subtreeTracker));
    }

    private  ArticulationPointStreamResult createResult(long node, long originalId, Optional<SubtreeTracker> subtreeTracker){
           return  subtreeTracker.map(
               tracker -> {
                   var resultMap =Map.of("max",tracker.maxComponentSize(node),
                             "min", tracker.minComponentSize(node),
                            "count", tracker.remainingComponents(node)
                   );
                   return new ArticulationPointStreamResult(originalId,resultMap);
               }
           ).orElse(new ArticulationPointStreamResult(originalId,null));
    }
}
