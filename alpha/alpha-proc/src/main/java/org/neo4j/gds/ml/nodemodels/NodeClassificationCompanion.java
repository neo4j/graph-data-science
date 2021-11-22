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

import org.neo4j.gds.GraphStoreValidation;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.validation.AfterLoadValidation;
import org.neo4j.gds.validation.ValidationConfiguration;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

final class NodeClassificationCompanion {

    static <CONFIG extends NodeClassificationPredictConfig> ValidationConfiguration<CONFIG> getValidationConfig(ModelCatalog modelCatalog) {
        return new ValidationConfiguration<>() {
            @Override
            public List<AfterLoadValidation<CONFIG>> afterLoadValidations() {
                return List.of(
                    (graphStore, graphCreateConfig, config) -> {
                        if (config instanceof NodeClassificationMutateConfig) {
                            var mutateConfig = (NodeClassificationMutateConfig) config;
                            validateProperties(
                                mutateConfig.mutateProperty(),
                                mutateConfig.predictedProbabilityProperty()
                            );
                        }

                        if (config instanceof NodeClassificationPredictWriteConfig) {
                            var writeConfig = (NodeClassificationPredictWriteConfig) config;
                            validateProperties(writeConfig.writeProperty(), writeConfig.predictedProbabilityProperty());
                        }

                        var trainConfig = modelCatalog.get(
                            config.username(),
                            config.modelName(),
                            NodeLogisticRegressionData.class,
                            NodeClassificationTrainConfig.class,
                            NodeClassificationModelInfo.class
                        ).trainConfig();
                        GraphStoreValidation.validate(graphStore, trainConfig);
                    }
                );
            }
        };
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

    private NodeClassificationCompanion() {}
}
