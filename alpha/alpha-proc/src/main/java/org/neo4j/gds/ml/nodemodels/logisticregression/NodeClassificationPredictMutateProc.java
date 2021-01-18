/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.NodeClassificationPredict;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.NodeClassificationPredictAlgorithmFactory;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.NodeClassificationPropertyPredictMutateConfig;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRResult;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.MutatePropertyProc;
import org.neo4j.graphalgo.api.GraphStoreValidation;
import org.neo4j.graphalgo.api.nodeproperties.DoubleArrayNodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreWithConfig;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.core.write.NodePropertyExporter.NodeProperty;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.StandardMutateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class NodeClassificationPredictMutateProc
    extends MutatePropertyProc<NodeClassificationPredict, MultiClassNLRResult, NodeClassificationPredictMutateProc.MutateResult, NodeClassificationPropertyPredictMutateConfig> {

    @Procedure(name = "gds.alpha.ml.nodeClassification.predict.mutate", mode = Mode.READ)
    @Description("Predicts classes for all nodes based on a previously trained model")
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(graphNameOrConfig, configuration);
        return mutate(result);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<NodeClassificationPredict, MultiClassNLRResult, NodeClassificationPropertyPredictMutateConfig> computeResult
    ) {
        return new MutateResult.Builder();
    }

    @Override
    protected void validateConfigsAndGraphStore(
        GraphStoreWithConfig graphStoreWithConfig, NodeClassificationPropertyPredictMutateConfig config
    ) {
        config.predictedProbabilityProperty().ifPresent(predictedProbabilityProperty -> {
            if (config.mutateProperty().equals(predictedProbabilityProperty)) {
                throw new IllegalArgumentException(
                    formatWithLocale(
                        "Configuration parameters `%s` and `%s` must be different (both were `%s`)",
                        "mutateProperty",
                        "predictedProbabilityProperty",
                        predictedProbabilityProperty
                    )
                );
            }
        });

        var trainConfig = ModelCatalog.get(
            config.username(),
            config.modelName(),
            MultiClassNLRData.class,
            NodeClassificationTrainConfig.class
        ).trainConfig();
        GraphStoreValidation.validate(
            graphStoreWithConfig,
            trainConfig
        );
    }

    @Override
    protected NodeClassificationPropertyPredictMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return NodeClassificationPropertyPredictMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<NodeClassificationPredict, NodeClassificationPropertyPredictMutateConfig> algorithmFactory() {
        return new NodeClassificationPredictAlgorithmFactory();
    }

    @Override
    protected List<NodeProperty> nodePropertyList(
        ComputationResult<NodeClassificationPredict, MultiClassNLRResult, NodeClassificationPropertyPredictMutateConfig> computationResult
    ) {
        var config = computationResult.config();
        var mutateProperty = config.mutateProperty();
        var result = computationResult.result();
        var classProperties = result.predictedClasses().asNodeProperties();
        var nodeProperties = new ArrayList<NodeProperty>();
        nodeProperties.add(NodeProperty.of(mutateProperty, classProperties));
        if (result.predictedProbabilities().isPresent()) {
            var probabilityPropertyKey = config.predictedProbabilityProperty().orElseThrow();
            var probabilityProperties = result.predictedProbabilities().get();
            nodeProperties.add(NodeProperty.of(
                probabilityPropertyKey,
                (DoubleArrayNodeProperties) probabilityProperties::get
            ));
        }

        return nodeProperties;
    }

    public static final class MutateResult extends StandardMutateResult {

        public final long nodePropertiesWritten;

        MutateResult(
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                createMillis,
                computeMillis,
                0L,
                mutateMillis,
                configuration
            );
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends AbstractResultBuilder<NodeClassificationPredictMutateProc.MutateResult> {

            @Override
            public NodeClassificationPredictMutateProc.MutateResult build() {
                return new NodeClassificationPredictMutateProc.MutateResult(
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }

}
