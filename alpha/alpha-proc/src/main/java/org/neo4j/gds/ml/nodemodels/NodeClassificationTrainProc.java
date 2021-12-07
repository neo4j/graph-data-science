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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.TrainProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.utils.StringJoining;
import org.neo4j.gds.validation.AfterLoadValidation;
import org.neo4j.gds.validation.ValidationConfiguration;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeClassificationTrainProc extends TrainProc<NodeClassificationTrain, NodeLogisticRegressionData, NodeClassificationTrainConfig, NodeClassificationModelInfo> {

    @Procedure(name = "gds.alpha.ml.nodeClassification.train", mode = Mode.READ)
    @Description("Trains a node classification model")
    public Stream<MLTrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return trainAndStoreModelWithResult(
            graphName, configuration,
            (model, result) -> new MLTrainResult(model, result.computeMillis())
        );
    }

    @Procedure(name = "gds.alpha.ml.nodeClassification.train.estimate", mode = Mode.READ)
    @Description("Trains a node classification model")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected String modelType() {
        return NodeClassificationTrain.MODEL_TYPE;
    }

    @Override
    public ValidationConfiguration<NodeClassificationTrainConfig> getValidationConfig() {
        return new ValidationConfiguration<>() {
            @Override
            public List<AfterLoadValidation<NodeClassificationTrainConfig>> afterLoadValidations() {
                return List.of(
                    (graphStore, graphCreateConfig, config) -> {
                        Collection<NodeLabel> filterLabels = config.nodeLabelIdentifiers(graphStore);
                        if (!graphStore.hasNodeProperty(filterLabels, config.targetProperty())) {
                            throw new IllegalArgumentException(formatWithLocale(
                                "`%s`: `%s` not found in graph with node properties: %s",
                                "targetProperty",
                                config.targetProperty(),
                                StringJoining.join(graphStore.nodePropertyKeys(filterLabels))
                            ));
                        }
                    }
                );
            }
        };
    }

    @Override
    protected NodeClassificationTrainConfig newConfig(String username, CypherMapWrapper config) {
        return NodeClassificationTrainConfig.of(username, config);
    }

    @Override
    protected GraphAlgorithmFactory<NodeClassificationTrain, NodeClassificationTrainConfig> algorithmFactory() {
        return new NodeClassificationTrainAlgorithmFactory();
    }

}
