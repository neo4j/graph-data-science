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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.ml.pipeline.ImmutablePipelineGraphFilter;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class NodeClassificationPredictPipelineStreamResultBuilder implements StreamResultBuilder<NodeClassificationPipelineResult, NodeClassificationStreamResult> {
    private final NodeClassificationPredictPipelineStreamConfig configuration;

    NodeClassificationPredictPipelineStreamResultBuilder(NodeClassificationPredictPipelineStreamConfig configuration) {this.configuration = configuration;}

    @Override
    public Stream<NodeClassificationStreamResult> build(
        Graph unused,
        GraphStore graphStore,
        Optional<NodeClassificationPipelineResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var pipelineGraphFilter = ImmutablePipelineGraphFilter.builder()
            .nodeLabels(configuration.nodeLabelIdentifiers(graphStore))
            .relationshipTypes(configuration.internalRelationshipTypes(graphStore))
            .build();

        var graph = graphStore.getGraph(pipelineGraphFilter.nodeLabels());

        var nodeClassificationPipelineResult = result.get();
        var predictedClasses = nodeClassificationPipelineResult.predictedClasses();
        var predictedProbabilities = nodeClassificationPipelineResult.predictedProbabilities();

        return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
            .mapToObj(nodeId -> new NodeClassificationStreamResult(
                graph.toOriginalNodeId(nodeId),
                predictedClasses.get(nodeId),
                nodePropertiesAsList(predictedProbabilities, nodeId)
            ));
    }

    private static List<Double> nodePropertiesAsList(
        Optional<HugeObjectArray<double[]>> predictedProbabilities,
        long nodeId
    ) {
        return predictedProbabilities.map(p -> {
            var values = p.get(nodeId);
            return Arrays.stream(values).boxed().collect(Collectors.toList());
        }).orElse(null);
    }
}
