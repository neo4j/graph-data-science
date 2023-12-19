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

import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.embeddings.specificfields.DoubleNodeEmbeddingsPropertyValues;
import org.neo4j.gds.algorithms.embeddings.specificfields.Node2VecSpecificFields;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;

public class NodeEmbeddingsAlgorithmsWriteBusinessFacade {

    private final NodeEmbeddingsAlgorithmsFacade nodeEmbeddingsAlgorithmsFacade;
    private final WriteNodePropertyService writeNodePropertyService;

    public NodeEmbeddingsAlgorithmsWriteBusinessFacade(
        NodeEmbeddingsAlgorithmsFacade nodeEmbeddingsAlgorithmsFacade,
        WriteNodePropertyService writeNodePropertyService
    ) {
        this.nodeEmbeddingsAlgorithmsFacade = nodeEmbeddingsAlgorithmsFacade;
        this.writeNodePropertyService = writeNodePropertyService;
    }

    public NodePropertyWriteResult<Node2VecSpecificFields> node2Vec(
        String graphName,
        Node2VecWriteConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.node2Vec(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        var writeResultBuilder = NodePropertyWriteResult.<Node2VecSpecificFields>builder()
            .computeMillis(intermediateResult.computeMilliseconds)
            .postProcessingMillis(0L)
            .configuration(configuration);

        algorithmResult.result().ifPresentOrElse(
            result -> {
                var nodeCount = algorithmResult.graph().nodeCount();
                var nodeProperties = new FloatEmbeddingNodePropertyValues(result.embeddings());
                var writeResult = writeNodePropertyService.write(
                    algorithmResult.graph(),
                    algorithmResult.graphStore(),
                    nodeProperties,
                    configuration.writeConcurrency(),
                    configuration.writeProperty(),
                    "Node2VecWrite",
                    configuration.arrowConnectionInfo()
                );
                writeResultBuilder.writeMillis(writeResult.writeMilliseconds());
                writeResultBuilder.nodePropertiesWritten(writeResult.nodePropertiesWritten());
                writeResultBuilder.algorithmSpecificFields(new Node2VecSpecificFields(nodeCount,result.lossPerIteration()));
            },
            () -> writeResultBuilder.algorithmSpecificFields(Node2VecSpecificFields.EMPTY)
        );

        return writeResultBuilder.build();
    }

    public NodePropertyWriteResult<Long> graphSage(
        String graphName,
        GraphSageWriteConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.graphSage(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        var writeResultBuilder = NodePropertyWriteResult.<Long>builder()
            .computeMillis(intermediateResult.computeMilliseconds)
            .postProcessingMillis(0L)
            .configuration(configuration);

        algorithmResult.result().ifPresentOrElse(
            result -> {
                var nodeCount = algorithmResult.graph().nodeCount();
                var nodeProperties = new DoubleNodeEmbeddingsPropertyValues(result.embeddings());
                var writeResult = writeNodePropertyService.write(
                    algorithmResult.graph(),
                    algorithmResult.graphStore(),
                    nodeProperties,
                    configuration.writeConcurrency(),
                    configuration.writeProperty(),
                    "GraphSageWrite",
                    configuration.arrowConnectionInfo()
                );
                writeResultBuilder.writeMillis(writeResult.writeMilliseconds());
                writeResultBuilder.nodePropertiesWritten(writeResult.nodePropertiesWritten());
                writeResultBuilder.algorithmSpecificFields(nodeCount);
            },
            () -> writeResultBuilder.algorithmSpecificFields(0l)
        );

        return writeResultBuilder.build();
    }


}
