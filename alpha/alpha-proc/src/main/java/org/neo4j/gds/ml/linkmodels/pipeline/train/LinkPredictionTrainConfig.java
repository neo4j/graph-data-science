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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.immutables.value.Value;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.model.ModelConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface LinkPredictionTrainConfig extends AlgoBaseConfig, ModelConfig, RandomSeedConfig {

    @Value.Default
    @Configuration.DoubleRange(min = 0, minInclusive = false)
    default double negativeClassWeight() {
        return 1.0;
    }

    String pipeline();

    @Configuration.Ignore
    @Value.Default
    default List<LinkMetric> metrics() {
        return List.of(LinkMetric.AUCPR);
    }

    @Configuration.GraphStoreValidationCheck
    default void validateTargetRelIsUndirected(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        var invalidTypes = selectedRelationshipTypes.stream()
            .filter(type -> !graphStore.isUndirected(type))
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        if (!invalidTypes.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "RelationshipTypes must be undirected, but found %s which are directed.",
                StringJoining.join(invalidTypes)
            ));
        }
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

    @TestOnly
    static ImmutableLinkPredictionTrainConfig.Builder builder() {
        return ImmutableLinkPredictionTrainConfig.builder();
    }
}
