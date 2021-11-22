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

import org.neo4j.gds.GraphStoreValidation;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.validation.AfterLoadValidation;
import org.neo4j.gds.validation.ValidationConfiguration;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeClassificationPipelineCompanion {

    static <CONFIG extends NodeClassificationPredictPipelineBaseConfig> ValidationConfiguration<CONFIG> getValidationConfig(
        ModelCatalog modelCatalog
    ) {
        return new ValidationConfiguration<>() {
            @Override
            public List<AfterLoadValidation<CONFIG>> afterLoadValidations() {
                return List.of(
                    (graphStore, graphCreateConfig, config) -> {
                        if (config instanceof NodeClassificationPredictPipelineMutateConfig) {
                            var mutateConfig = (NodeClassificationPredictPipelineMutateConfig) config;

                            validatePredictedProbabilityPropertyDoesNotExist(
                                graphStore,
                                mutateConfig.nodeLabelIdentifiers(graphStore),
                                mutateConfig.predictedProbabilityProperty()
                            );

                            validateProperties(
                                mutateConfig.mutateProperty(),
                                mutateConfig.predictedProbabilityProperty()
                            );
                        }

                        var trainConfig = modelCatalog.get(
                            config.username(),
                            config.modelName(),
                            NodeLogisticRegressionData.class,
                            NodeClassificationPipelineTrainConfig.class,
                            NodeClassificationPipelineModelInfo.class
                        ).trainConfig();
                        GraphStoreValidation.validate(graphStore, trainConfig);
                    }
                );
            }
        };
    }

    private static void validatePredictedProbabilityPropertyDoesNotExist(
        GraphStore graphStore,
        Collection<NodeLabel> filterLabels,
        Optional<String> maybePredictedProbabilityProperty
    ) {
        maybePredictedProbabilityProperty.ifPresent(predictedProbabilityProperty -> {
            if (graphStore.hasNodeProperty(filterLabels, predictedProbabilityProperty)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node property `%s` already exists in the in-memory graph.",
                    predictedProbabilityProperty
                ));
            }
        });
    }

    private static void validateProperties(String property, Optional<String> maybePredictedProbabilityProperty) {
        maybePredictedProbabilityProperty.ifPresent(predictedProbabilityProperty -> {
            if (property.equals(predictedProbabilityProperty)) {
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
    }


    private NodeClassificationPipelineCompanion() {

    }
}
