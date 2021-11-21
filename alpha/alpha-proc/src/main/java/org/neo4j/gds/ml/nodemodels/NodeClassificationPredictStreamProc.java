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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphStoreValidation;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeClassificationResult;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class NodeClassificationPredictStreamProc
    extends StreamProc<NodeClassificationPredict, NodeClassificationResult, NodeClassificationStreamResult, NodeClassificationStreamConfig> {

    @Context
    public ModelCatalog modelCatalog;

    @Procedure(name = "gds.alpha.ml.nodeClassification.predict.stream", mode = Mode.READ)
    @Description("Predicts classes for all nodes based on a previously trained model")
    public Stream<NodeClassificationStreamResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(graphNameOrConfig, configuration);
        return stream(result);
    }

    @Procedure(name = "gds.alpha.ml.nodeClassification.predict.stream.estimate", mode = Mode.READ)
    @Description("Predicts classes for all nodes based on a previously trained model")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected Stream<NodeClassificationStreamResult> stream(
        ComputationResult<
        NodeClassificationPredict, NodeClassificationResult, NodeClassificationStreamConfig
        > computationResult
    ) {
        return runWithExceptionLogging("Graph streaming failed", () -> {
            Graph graph = computationResult.graph();

            var result = computationResult.result();
            var predictedClasses = result.predictedClasses();
            var predictedProbabilities = result.predictedProbabilities();
            return LongStream
                .range(0, graph.nodeCount())
                .boxed()
                .map((nodeId) ->
                    new NodeClassificationStreamResult(
                        graph.toOriginalNodeId(nodeId),
                        predictedClasses.get(nodeId),
                        nodePropertiesAsList(predictedProbabilities, nodeId)
                    )
                );
        });
    }

    private List<Double> nodePropertiesAsList(Optional<HugeObjectArray<double[]>> predictedProbabilities, long nodeId) {
        return predictedProbabilities.map(p -> {
            var values = p.get(nodeId);
            return Arrays.stream(values).boxed().collect(Collectors.toList());
        }).orElse(null);
    }

    @Override
    protected void validateConfigsAfterLoad(
        GraphStore graphStore, GraphCreateConfig graphCreateConfig, NodeClassificationStreamConfig config
    ) {
        super.validateConfigsAfterLoad(graphStore, graphCreateConfig, config);
        var trainConfig = modelCatalog.get(
            config.username(),
            config.modelName(),
            NodeLogisticRegressionData.class,
            NodeClassificationTrainConfig.class,
            NodeClassificationModelInfo.class
        ).trainConfig();
        GraphStoreValidation.validate(graphStore, trainConfig);
    }

    @Override
    protected NodeClassificationStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return NodeClassificationStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<NodeClassificationPredict, NodeClassificationStreamConfig> algorithmFactory() {
        return new NodeClassificationPredictAlgorithmFactory<>(modelCatalog);
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<NodeClassificationPredict, NodeClassificationResult, NodeClassificationStreamConfig> computationResult) {
        return super.nodeProperties(computationResult);
    }

    @Override
    protected NodeClassificationStreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        throw new UnsupportedOperationException("NodeClassification handles result building individually.");
    }
}
