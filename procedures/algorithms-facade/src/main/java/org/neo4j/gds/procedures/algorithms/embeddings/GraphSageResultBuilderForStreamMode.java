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
package org.neo4j.gds.procedures.algorithms.embeddings;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class GraphSageResultBuilderForStreamMode implements ResultBuilder<GraphSageStreamConfig, GraphSageResult, Stream<GraphSageStreamResult>, Void> {
    @Override
    public Stream<GraphSageStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        GraphSageStreamConfig configuration,
        Optional<GraphSageResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Void> unused
    ) {
        if (result.isEmpty()) return Stream.empty();

        var graphSageResult = result.get();

        var embeddings = graphSageResult.embeddings();

        return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
            .mapToObj(internalNodeId -> new GraphSageStreamResult(
                graph.toOriginalNodeId(internalNodeId),
                embeddings.get(internalNodeId)
            ));
    }
}
