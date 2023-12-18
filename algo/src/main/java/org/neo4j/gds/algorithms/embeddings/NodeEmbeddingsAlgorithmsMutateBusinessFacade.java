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

import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.embeddings.specificfields.Node2VecSpecificFields;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;

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
        var algorithmResult = intermediateResult.algorithmResult;

        var mutateResultBuilder = NodePropertyMutateResult.<Node2VecSpecificFields>builder()
            .computeMillis(intermediateResult.computeMilliseconds)
            .postProcessingMillis(0L)
            .configuration(configuration);

        algorithmResult.result().ifPresentOrElse(
            result -> {
                var nodeCount = algorithmResult.graph().nodeCount();
                var nodeProperties = new EmbeddingNodePropertyValues(result.embeddings());
                var mutateResult = mutateNodePropertyService.mutate(
                    configuration.mutateProperty(),
                    nodeProperties,
                    configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                    algorithmResult.graph(),
                    algorithmResult.graphStore()
                );
                mutateResultBuilder.mutateMillis(mutateResult.mutateMilliseconds());
                mutateResultBuilder.nodePropertiesWritten(mutateResult.nodePropertiesAdded());
                mutateResultBuilder.algorithmSpecificFields(new Node2VecSpecificFields(nodeCount,result.lossPerIteration()));
            },
            () -> mutateResultBuilder.algorithmSpecificFields(Node2VecSpecificFields.EMPTY)
        );

        return mutateResultBuilder.build();
    }

}
