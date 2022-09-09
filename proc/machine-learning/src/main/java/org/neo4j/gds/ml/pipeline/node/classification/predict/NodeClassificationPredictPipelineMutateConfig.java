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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.immutables.value.Value;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.config.MutatePropertyConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
@SuppressWarnings("immutables:subtype")
public interface NodeClassificationPredictPipelineMutateConfig
    extends NodeClassificationPredictPipelineBaseConfig, MutatePropertyConfig
{
    @Override
    @Value.Derived
    @Configuration.Ignore
    default boolean includePredictedProbabilities() {
        return predictedProbabilityProperty().isPresent();
    }

    Optional<String> predictedProbabilityProperty();

    static NodeClassificationPredictPipelineMutateConfig of(String username, CypherMapWrapper config) {
        return new NodeClassificationPredictPipelineMutateConfigImpl(username, config);
    }

    @Value.Check
    default void validateMutatePropertiesDiffer() {
        predictedProbabilityProperty().ifPresent(predictedProbabilityProperty -> {
            if (mutateProperty().equals(predictedProbabilityProperty)) {
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

    @Override
    @Configuration.Ignore
    default Collection<NodeLabel> nodeLabelIdentifiers(GraphStore graphStore) {
        return ElementTypeValidator.resolve(graphStore, targetNodeLabels());
    }

    @Configuration.GraphStoreValidationCheck
    @Value.Default
    default void validatePredictedProbabilityPropertyDoesNotExist(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        predictedProbabilityProperty().ifPresent(predictedProbabilityProperty -> {
            if (graphStore.hasNodeProperty(selectedLabels, predictedProbabilityProperty)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node property `%s` already exists in the in-memory graph.",
                    predictedProbabilityProperty
                ));
            }
        });
    }
}
