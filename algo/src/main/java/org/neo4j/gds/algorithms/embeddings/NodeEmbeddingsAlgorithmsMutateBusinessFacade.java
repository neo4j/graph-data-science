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
package org.neo4j.gds.algorithms.embeddings;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.embeddings.specificfields.Node2VecSpecificFields;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;

import java.util.function.Function;
import java.util.function.Supplier;

public class NodeEmbeddingsAlgorithmsMutateBusinessFacade {
    private final NodeEmbeddingsAlgorithmsFacade nodeEmbeddingsAlgorithmsFacade;
    private final MutateNodePropertyService mutateNodePropertyService;

    public NodeEmbeddingsAlgorithmsMutateBusinessFacade(NodeEmbeddingsAlgorithmsFacade communityAlgorithmsFacade,MutateNodePropertyService mutateNodePropertyService) {
        this.nodeEmbeddingsAlgorithmsFacade = communityAlgorithmsFacade;
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    public NodePropertyMutateResult<Node2VecSpecificFields> node2Vec(String graphName, Node2VecMutateConfig configuration){

        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.node2Vec(graphName, configuration)
        );

        return mutateNodeProperty(
            intermediateResult.algorithmResult,
            configuration,
            (result) -> new FloatEmbeddingNodePropertyValues(result.embeddings()),
            (result) -> new Node2VecSpecificFields(
                intermediateResult.algorithmResult.graph().nodeCount(),
                result.lossPerIteration()
            ),
            intermediateResult.computeMilliseconds,
            () -> Node2VecSpecificFields.EMPTY
        );
    }

    public NodePropertyMutateResult<Long> graphSage(
        String graphName,
        GraphSageMutateConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.graphSage(graphName, configuration)
        );

        return mutateNodeProperty(
            intermediateResult.algorithmResult,
            configuration,
            (result) -> NodePropertyValuesAdapter.adapt(result.embeddings()),
            (result) -> new Long(intermediateResult.algorithmResult.graph().nodeCount()),
            intermediateResult.computeMilliseconds,
            () -> new Long(0)
        );
    }

    public NodePropertyMutateResult<Long> fastRP(
        String graphName,
        FastRPMutateConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.fastRP(graphName, configuration)
        );

        return mutateNodeProperty(
            intermediateResult.algorithmResult,
            configuration,
            (result) -> NodePropertyValuesAdapter.adapt(result.embeddings()),
            (result) -> new Long(intermediateResult.algorithmResult.graph().nodeCount()),
            intermediateResult.computeMilliseconds,
            () -> new Long(0)

        );
    }

    <RESULT, CONFIG extends MutateNodePropertyConfig, ASF> NodePropertyMutateResult<ASF> mutateNodeProperty(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        Function<RESULT, NodePropertyValues> nodePropertyValuesSupplier,
        Function<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier
    ) {
        return algorithmResult.result().map(result -> {
            //get node properties
            var nodePropertyValues = nodePropertyValuesSupplier.apply(result);
            // 3. Go and mutate the graph store
            var addNodePropertyResult = mutateNodePropertyService.mutate(
                configuration.mutateProperty(),
                nodePropertyValues,
                configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                algorithmResult.graph(), algorithmResult.graphStore()
            );

            var specificFields = specificFieldsSupplier.apply(result);

            return NodePropertyMutateResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(0)
                .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
                .mutateMillis(addNodePropertyResult.mutateMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyMutateResult.empty(emptyASFSupplier.get(), configuration));

    }
}
