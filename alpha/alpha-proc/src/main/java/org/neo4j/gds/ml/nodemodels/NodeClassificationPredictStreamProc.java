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

import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.NodeLogisticRegressionResult;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStoreValidation;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreWithConfig;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
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
    extends StreamProc<NodeClassificationPredict, NodeLogisticRegressionResult, NodeClassificationPredictStreamProc.StreamResult, NodeClassificationStreamConfig> {

    @Procedure(name = "gds.alpha.ml.nodeClassification.predict.stream", mode = Mode.READ)
    @Description("Predicts classes for all nodes based on a previously trained model")
    public Stream<StreamResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(graphNameOrConfig, configuration);
        return stream(result);
    }

    @Override
    protected Stream<StreamResult> stream(ComputationResult<NodeClassificationPredict, NodeLogisticRegressionResult, NodeClassificationStreamConfig> computationResult) {
        return runWithExceptionLogging("Graph streaming failed", () -> {
            Graph graph = computationResult.graph();

            var result = computationResult.result();
            var predictedClasses = result.predictedClasses();
            var predictedProbabilities = result.predictedProbabilities();
            return LongStream
                .range(0, graph.nodeCount())
                .boxed()
                .map((nodeId) ->
                    new StreamResult(nodeId, predictedClasses.get(nodeId), nodePropertiesAsList(predictedProbabilities, nodeId)));
        });
    }

    private List<Double> nodePropertiesAsList(Optional<HugeObjectArray<double[]>> predictedProbabilities, long nodeId) {
        return predictedProbabilities.map(p -> {
            var values = p.get(nodeId);
            return Arrays.stream(values).boxed().collect(Collectors.toList());
        }).orElse(null);
    }

    @Override
    protected void validateConfigsAndGraphStore(
        GraphStoreWithConfig graphStoreWithConfig, NodeClassificationStreamConfig config
    ) {
        var trainConfig = ModelCatalog.get(
            config.username(),
            config.modelName(),
            NodeLogisticRegressionData.class,
            NodeClassificationTrainConfig.class
        ).trainConfig();
        GraphStoreValidation.validate(
            graphStoreWithConfig,
            trainConfig
        );
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
        return new NodeClassificationPredictAlgorithmFactory<>();
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<NodeClassificationPredict, NodeLogisticRegressionResult, NodeClassificationStreamConfig> computationResult) {
        return super.nodeProperties(computationResult);
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        throw new UnsupportedOperationException("NodeClassification handles result building individually.");
    }

    @SuppressWarnings("unused")
    public static final class StreamResult {

        public long nodeId;
        public long predictedClass;
        public List<Double> predictedProbabilities;

        StreamResult(long nodeId, long predictedClass) {
            this(nodeId, predictedClass, null);
        }

        StreamResult(long nodeId, long predictedClass, List<Double> predictedProbabilities) {
            this.nodeId = nodeId;
            this.predictedClass = predictedClass;
            this.predictedProbabilities = predictedProbabilities;
        }
    }
}
