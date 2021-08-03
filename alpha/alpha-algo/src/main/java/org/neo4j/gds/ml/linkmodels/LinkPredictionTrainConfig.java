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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.FeaturePropertiesConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.ModelConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface LinkPredictionTrainConfig extends AlgoBaseConfig, FeaturePropertiesConfig, ModelConfig, RelationshipWeightConfig, RandomSeedConfig {

    @Configuration.IntegerRange(min = 2)
    int validationFolds();

    @Configuration.DoubleRange(min = 0, minInclusive = false)
    double negativeClassWeight();

    List<Map<String, Object>> params();

    @Value.Derived
    @Configuration.Ignore
    default List<LinkLogisticRegressionTrainConfig> paramConfigs() {
        return params().stream().map(params -> LinkLogisticRegressionTrainConfig.of(
            featureProperties(),
            concurrency(),
            params
        )).collect(Collectors.toList());
    }

    @Configuration.ConvertWith("org.neo4j.graphalgo.RelationshipType#of")
    @Configuration.ToMapValue("org.neo4j.graphalgo.RelationshipType#toString")
    RelationshipType trainRelationshipType();

    @Configuration.ConvertWith("org.neo4j.graphalgo.RelationshipType#of")
    @Configuration.ToMapValue("org.neo4j.graphalgo.RelationshipType#toString")
    RelationshipType testRelationshipType();

    @Configuration.Ignore
    @Value.Default
    default List<LinkMetric> metrics() {
        return List.of(LinkMetric.AUCPR);
    }

    @Override
    @Configuration.Ignore
    @Value.Default
    default @Nullable String relationshipWeightProperty() {
        return null;
    }

    static LinkPredictionTrainConfig of(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return new LinkPredictionTrainConfigImpl(
            graphName,
            maybeImplicitCreate,
            username,
            config
        );
    }

    static ImmutableLinkPredictionTrainConfig.Builder builder() {
        return ImmutableLinkPredictionTrainConfig.builder();
    }
}
