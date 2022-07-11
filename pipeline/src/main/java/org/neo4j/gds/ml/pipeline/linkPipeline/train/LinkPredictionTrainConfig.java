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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.immutables.value.Value;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.metrics.LinkMetric;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.model.ModelConfig;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@SuppressWarnings("immutables:subtype")
public interface LinkPredictionTrainConfig extends AlgoBaseConfig, GraphNameConfig, ModelConfig, RandomSeedConfig {

    @Value.Default
    @Configuration.DoubleRange(min = 0, minInclusive = false)
    default double negativeClassWeight() {
        return 1.0;
    }

    String pipeline();

    String targetRelationshipType();

    String sourceNodeLabel();

    String targetNodeLabel();

    default List<String> contextRelationshipTypes() {
        return List.of();
    }

    @Override
    @Configuration.Ignore
    @Configuration.Key(RELATIONSHIP_TYPES_KEY)
    default List<String> relationshipTypes() {
        return Stream.concat(
                contextRelationshipTypes().stream(),
                Stream.of(targetRelationshipType())
            )
            .collect(Collectors.toList());
    }

    @Value.Check
    default void validate() {
        if (targetRelationshipType().equals(ElementProjection.PROJECT_ALL)) {
            throw new IllegalArgumentException("'*' is not allowed as targetRelationshipType.");
        }
    }

    @Configuration.Ignore
    default RelationshipType internalTargetRelationshipType() {
        return RelationshipType.of(targetRelationshipType());
    }

    default Collection<RelationshipType> internalContextRelationshipType(GraphStore graphStore) {
        return contextRelationshipTypes().contains(ElementProjection.PROJECT_ALL)
            ? graphStore.relationshipTypes()
            : contextRelationshipTypes().stream().map(RelationshipType::new).collect(Collectors.toList());
    }

    @Override
    @Configuration.Ignore
    default List<String> nodeLabels() {
        return Stream.of(sourceNodeLabel(), targetNodeLabel()).distinct().collect(Collectors.toList());
    }

    @Configuration.ConvertWith("org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig#namesToMetrics")
    @Configuration.ToMapValue("org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig#metricsToNames")
    default List<Metric> metrics() {
        return List.of(LinkMetric.AUCPR);
    }


    @Configuration.Ignore
    default Metric mainMetric() {
        return metrics().get(0);
    }

    @Configuration.Ignore
    default List<LinkMetric> linkMetrics() {
        return metrics()
            .stream()
            .filter(metric -> !metric.isModelSpecific())
            .map(metric -> (LinkMetric) metric)
            .collect(Collectors.toList());
    }

    static LinkPredictionTrainConfig of(String username, CypherMapWrapper config) {
        return new LinkPredictionTrainConfigImpl(username, config);
    }

    static List<Metric> namesToMetrics(List<?> names) {
        return names.stream().map(LinkMetric::parseLinkMetric).collect(Collectors.toList());
    }

    static List<String> metricsToNames(List<Metric> metrics) {
        return metrics.stream().map(Metric::name).collect(Collectors.toList());
    }
}
