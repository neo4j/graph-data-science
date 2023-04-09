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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.kmeans.Kmeans.KMEANS_DESCRIPTION;

@GdsCallable(name = "gds.beta.kmeans.stream", description = KMEANS_DESCRIPTION, executionMode = ExecutionMode.STREAM)
public class KmeansStreamSpec implements AlgorithmSpec<Kmeans, KmeansResult, KmeansStreamConfig, Stream<StreamResult>, KmeansAlgorithmFactory<KmeansStreamConfig>> {
    @Override
    public String name() {
        return "KmeansStream";
    }

    @Override
    public KmeansAlgorithmFactory<KmeansStreamConfig> algorithmFactory() {
        return new KmeansAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KmeansStreamConfig> newConfigFunction() {
        return (__, config) -> KmeansStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Kmeans, KmeansResult, KmeansStreamConfig, Stream<StreamResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {
            if(computationResult.result().isEmpty()) {
                return Stream.empty();
            }

            var result = computationResult.result().get();
            var communities = result.communities();
            var distances = result.distanceFromCenter();
            var silhuette = result.silhouette();
            var graph = computationResult.graph();
            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .mapToObj(nodeId -> new StreamResult(
                    graph.toOriginalNodeId(nodeId),
                    communities.get(nodeId),
                    distances.get(nodeId),
                    silhuette == null ? -1 : silhuette.get(nodeId)
                ));
        };
    }

}
