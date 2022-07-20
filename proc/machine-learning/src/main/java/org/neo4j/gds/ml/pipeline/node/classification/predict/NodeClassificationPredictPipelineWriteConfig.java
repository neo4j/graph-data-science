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
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
@SuppressWarnings("immutables:subtype")
public interface NodeClassificationPredictPipelineWriteConfig
    extends NodeClassificationPredictPipelineBaseConfig, WritePropertyConfig
{
    @Override
    @Value.Derived
    @Configuration.Ignore
    default boolean includePredictedProbabilities() {
        return predictedProbabilityProperty().isPresent();
    }

    Optional<String> predictedProbabilityProperty();

    @Value.Check
    default void validatePredictedProbabilityPropertyDoesNotExist() {
        predictedProbabilityProperty()
            .ifPresent(predictedProbabilityProperty -> {
                if (writeProperty().equals(predictedProbabilityProperty)) {
                    throw new IllegalArgumentException(
                        formatWithLocale(
                            "Configuration parameters `%s` and `%s` must be different (both were `%s`)",
                            "writeProperty",
                            "predictedProbabilityProperty",
                            predictedProbabilityProperty
                        )
                    );
                }
            });
    }

    static NodeClassificationPredictPipelineWriteConfig of(String username, CypherMapWrapper config) {
        return new NodeClassificationPredictPipelineWriteConfigImpl(username, config);
    }
}
