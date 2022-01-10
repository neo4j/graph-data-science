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
package org.neo4j.gds.ml.linkmodels;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.TrainProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.GraphProjectConfigValidations;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.ml.MLTrainResult;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdsCallable(name = "gds.alpha.ml.linkPrediction.train", description = "Trains a link prediction model", executionMode = TRAIN)
public class LinkPredictionTrainProc extends TrainProc<LinkPredictionTrain, LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo, MLTrainResult> {

    @Procedure(name = "gds.alpha.ml.linkPrediction.train", mode = Mode.READ)
    @Description("Trains a link prediction model")
    public Stream<MLTrainResult> train(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return trainAndStoreModelWithResult(
            compute(graphName, configuration)
        );
    }

    @Procedure(name = "gds.alpha.ml.linkPrediction.train.estimate", mode = Mode.READ)
    @Description("Estimates memory for training a link prediction model")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LinkPredictionTrainConfig newConfig(String username, CypherMapWrapper config) {
        var lpConfig = LinkPredictionTrainConfig.of(username, config);
        var trainType = lpConfig.trainRelationshipType();
        var testType = lpConfig.testRelationshipType();
        return ImmutableLinkPredictionTrainConfig
            .builder()
            .from(lpConfig)
            .relationshipTypes(List.of(trainType.name, testType.name))
            .relationshipWeightProperty(EdgeSplitter.RELATIONSHIP_PROPERTY)
            .build();
    }

    @Override
    protected String modelType() {
        return LinkPredictionTrain.MODEL_TYPE;
    }

    @Override
    public ValidationConfiguration<LinkPredictionTrainConfig> validationConfig() {
        return new ValidationConfiguration<>() {
            @Override
            public List<BeforeLoadValidation<LinkPredictionTrainConfig>> beforeLoadValidations() {
                return List.of(
                    new GraphProjectConfigValidations.UndirectedGraphValidation<>(),
                    (graphProjectConfig, config) -> {
                        if (config.params().isEmpty()) {
                            throw new IllegalArgumentException(formatWithLocale(
                                "No model candidates (params) specified, we require at least one"));
                        }
                    }
                );
            }
        };
    }

    @Override
    public GraphAlgorithmFactory<LinkPredictionTrain, LinkPredictionTrainConfig> algorithmFactory() {
        return new LinkPredictionTrainFactory();
    }

    @Override
    protected MLTrainResult constructResult(
        Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> model,
        ComputationResult<LinkPredictionTrain, Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo>, LinkPredictionTrainConfig> computationResult
    ) {
        return new MLTrainResult(model, computationResult.computeMillis());
    }
}
