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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.FeaturePropertiesConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.ModelConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.config.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.config.StringIdentifierValidations.validateNoWhiteCharacter;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface NodeClassificationTrainConfig extends AlgoBaseConfig, FeaturePropertiesConfig, ModelConfig, RandomSeedConfig {

    long serialVersionUID = 0x42L;

    @Configuration.ConvertWith("org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification#parse")
    @Configuration.ToMapValue("org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification#specificationsToString")
    List<MetricSpecification> metrics();

    @Configuration.DoubleRange(min = 0, max = 1)
    double holdoutFraction();

    @Configuration.IntegerRange(min = 2)
    int validationFolds();

    @Configuration.ConvertWith("validateProperty")
    String targetProperty();

    List<Map<String, Object>> params();

    @Value.Derived
    @Configuration.Ignore
    default List<NodeLogisticRegressionTrainConfig> paramsConfig() {
        return params()
            .stream()
            .map(map ->
                NodeLogisticRegressionTrainConfig.of(
                    featureProperties(),
                    targetProperty(),
                    map
                )
            ).collect(Collectors.toList());
    }

    @Value.Check
    default void validateParametersAreNotEmpty() {
        if (params().isEmpty()) {
            throw new IllegalArgumentException("No model candidates (params) specified, we require at least one");
        }
    }

    static @Nullable String validateProperty(String input) {
        return validateNoWhiteCharacter(emptyToNull(input), "targetProperty");
    }

    static NodeClassificationTrainConfig of(
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        String username,
        CypherMapWrapper config
    ) {
        return new NodeClassificationTrainConfigImpl(
            graphName,
            maybeImplicitCreate,
            username,
            config
        );
    }

    static ImmutableNodeClassificationTrainConfig.Builder builder() {
        return ImmutableNodeClassificationTrainConfig.builder();
    }
}
