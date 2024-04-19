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
import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.embeddings.specificfields.Node2VecSpecificFields;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

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

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            (result) -> new FloatEmbeddingNodePropertyValues(result.embeddings()),
            (result) -> new Node2VecSpecificFields(
                intermediateResult.algorithmResult.graph().nodeCount(),
                result.lossPerIteration()
            ),
            intermediateResult.computeMilliseconds,
            () -> Node2VecSpecificFields.EMPTY,
            "Node2VecWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(intermediateResult.algorithmResult.graphStore().resultStore())
        );
    }

    public NodePropertyWriteResult<Long> graphSage(
        String graphName,
        GraphSageWriteConfig configuration
    ) {

        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.graphSage(graphName, configuration)
        );

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            (result) -> NodePropertyValuesAdapter.adapt(result.embeddings()),
            (result) -> intermediateResult.algorithmResult.graph().nodeCount(),
            intermediateResult.computeMilliseconds,
            () -> 0l,
            "GraphSageWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(intermediateResult.algorithmResult.graphStore().resultStore())
        );
    }

    public NodePropertyWriteResult<Long> fastRP(
        String graphName,
        FastRPWriteConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.fastRP(graphName, configuration)
        );

        return writeToDatabase(
            intermediateResult.algorithmResult,
            configuration,
            (result) -> NodePropertyValuesAdapter.adapt(result.embeddings()),
            (result) -> intermediateResult.algorithmResult.graph().nodeCount(),
            intermediateResult.computeMilliseconds,
            () -> 0L,
            "FastRPWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),

            configuration.resolveResultStore(intermediateResult.algorithmResult.graphStore().resultStore())
        );
    }

    <RESULT, CONFIG extends AlgoBaseConfig, ASF> NodePropertyWriteResult<ASF> writeToDatabase(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        Function<RESULT, NodePropertyValues> nodePropertyValuesSupplier,
        Function<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        String procedureName,
        Concurrency writeConcurrency,
        String writeProperty,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        Optional<ResultStore> resultStore
    ) {

        return algorithmResult.result().map(result -> {

            //get node properties
            var nodePropertyValues = nodePropertyValuesSupplier.apply(result);


            // 3. Write to database
            var writeNodePropertyResult = writeNodePropertyService.write(
                algorithmResult.graph(),
                algorithmResult.graphStore(),
                nodePropertyValues,
                writeConcurrency,
                writeProperty,
                procedureName,
                arrowConnectionInfo,
                resultStore
            );


            var specificFields = specificFieldsSupplier.apply(result);

            return NodePropertyWriteResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(0)
                .nodePropertiesWritten(writeNodePropertyResult.nodePropertiesWritten())
                .writeMillis(writeNodePropertyResult.writeMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyWriteResult.empty(emptyASFSupplier.get(), configuration));

    }


}
