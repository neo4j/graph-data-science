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
package org.neo4j.gds.ml.nodemodels.pipeline.predict;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.WriteProc;
import org.neo4j.gds.api.nodeproperties.DoubleArrayNodeProperties;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.AfterLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredict;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardWriteResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.ml.PipelineCompanion.prepareTrainConfig;
import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion.ESTIMATE_PREDICT_DESCRIPTION;
import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion.PREDICT_DESCRIPTION;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdsCallable(name = "gds.beta.pipeline.nodeClassification.predict.write", description = PREDICT_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class NodeClassificationPipelineWriteProc
    extends WriteProc<
    NodeClassificationPredictPipelineExecutor,
    NodeClassificationPredict.NodeClassificationResult,
    NodeClassificationPipelineWriteProc.WriteResult,
    NodeClassificationPredictPipelineWriteConfig>
{

    @Procedure(name = "gds.beta.pipeline.nodeClassification.predict.write", mode = Mode.WRITE)
    @Description(PREDICT_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        prepareTrainConfig(graphName, configuration);
        return write(compute(graphName, configuration));
    }

    @Procedure(name = "gds.beta.pipeline.nodeClassification.predict.write.estimate", mode = Mode.READ)
    @Description(ESTIMATE_PREDICT_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        prepareTrainConfig(graphNameOrConfiguration, algoConfiguration);
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    public ValidationConfiguration<NodeClassificationPredictPipelineWriteConfig> validationConfig() {
        return new ValidationConfiguration<>() {
            @Override
            public List<AfterLoadValidation<NodeClassificationPredictPipelineWriteConfig>> afterLoadValidations() {
                return List.of(
                    (graphStore, graphProjectConfig, config) -> config.predictedProbabilityProperty()
                        .ifPresent(predictedProbabilityProperty -> {
                            if (config.writeProperty().equals(predictedProbabilityProperty)) {
                                throw new IllegalArgumentException(
                                    formatWithLocale(
                                        "Configuration parameters `%s` and `%s` must be different (both were `%s`)",
                                        "writeProperty",
                                        "predictedProbabilityProperty",
                                        predictedProbabilityProperty
                                    )
                                );
                            }
                        })
                );
            }
        };
    }

    @Override
    public AlgorithmSpec<NodeClassificationPredictPipelineExecutor, NodeClassificationPredict.NodeClassificationResult, NodeClassificationPredictPipelineWriteConfig, Stream<WriteResult>, AlgorithmFactory<?, NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineWriteConfig>> withModelCatalog(
        ModelCatalog modelCatalog
    ) {
        this.setModelCatalog(modelCatalog);
        return this;
    }

    @Override
    protected List<NodeProperty> nodePropertyList(ComputationResult<NodeClassificationPredictPipelineExecutor, NodeClassificationPredict.NodeClassificationResult, NodeClassificationPredictPipelineWriteConfig> computationResult) {
        var config = computationResult.config();
        var writeProperty = config.writeProperty();
        var result = computationResult.result();
        var classProperties = result.predictedClasses().asNodeProperties();
        var nodeProperties = new ArrayList<NodeProperty>();
        nodeProperties.add(NodeProperty.of(writeProperty, classProperties));

        result.predictedProbabilities().ifPresent((probabilityProperties) -> {
            var properties = new DoubleArrayNodeProperties() {
                @Override
                public long size() {
                    return computationResult.graph().nodeCount();
                }

                @Override
                public double[] doubleArrayValue(long nodeId) {
                    return probabilityProperties.get(nodeId);
                }
            };

            nodeProperties.add(NodeProperty.of(
                config.predictedProbabilityProperty().orElseThrow(),
                properties
            ));
        });

        return nodeProperties;
    }


    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<NodeClassificationPredictPipelineExecutor, NodeClassificationPredict.NodeClassificationResult, NodeClassificationPredictPipelineWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new WriteResult.Builder();
    }

    @Override
    protected NodeClassificationPredictPipelineWriteConfig newConfig(String username, CypherMapWrapper config) {
        return NodeClassificationPredictPipelineWriteConfig.of(username, config);
    }

    @Override
    public GraphStoreAlgorithmFactory<NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineWriteConfig> algorithmFactory() {
        return new NodeClassificationPredictPipelineAlgorithmFactory<>(executionContext(), modelCatalog());
    }

    @SuppressWarnings("unused")
    public static final class WriteResult extends StandardWriteResult {

        public final long nodePropertiesWritten;

        WriteResult(
            long preProcessingMillis,
            long computeMillis,
            long writeMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                preProcessingMillis,
                computeMillis,
                0L,
                writeMillis,
                configuration
            );
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends AbstractResultBuilder<WriteResult> {

            @Override
            public WriteResult build() {
                return new WriteResult(
                    preProcessingMillis,
                    computeMillis,
                    writeMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }
}
