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
package org.neo4j.gds.ml.splitting;

import org.immutables.value.Value;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface SplitRelationshipsBaseConfig extends AlgoBaseConfig, RandomSeedConfig, RelationshipWeightConfig {

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    double holdoutFraction();

    @Configuration.DoubleRange(min = 0.0)
    double negativeSamplingRatio();

    default List<String> sourceNodeLabels() {
        return List.of(ElementProjection.PROJECT_ALL);
    };

    default List<String> targetNodeLabels() {
        return List.of(ElementProjection.PROJECT_ALL);
    };

    @Configuration.Ignore
    @Override
    default List<String> nodeLabels() {
        return Stream.of(sourceNodeLabels(), targetNodeLabels()).flatMap(List::stream).distinct().collect(Collectors.toList());
    }

    @Configuration.Ignore
    default Collection<NodeLabel> internalSourceLabels(GraphStore graphStore) {
        return sourceNodeLabels().contains(ElementProjection.PROJECT_ALL)
            ? graphStore.nodeLabels()
            : sourceNodeLabels().stream().map(NodeLabel::of).collect(Collectors.toList());
    }

    @Configuration.Ignore
    default Collection<NodeLabel> internalTargetLabels(GraphStore graphStore) {
        return targetNodeLabels().contains(ElementProjection.PROJECT_ALL)
            ? graphStore.nodeLabels()
            : targetNodeLabels().stream().map(NodeLabel::of).collect(Collectors.toList());
    }

    @Configuration.ConvertWith("org.neo4j.gds.RelationshipType#of")
    @Configuration.ToMapValue("org.neo4j.gds.RelationshipType#toString")
    RelationshipType holdoutRelationshipType();

    @Configuration.ConvertWith("org.neo4j.gds.RelationshipType#of")
    @Configuration.ToMapValue("org.neo4j.gds.RelationshipType#toString")
    RelationshipType remainingRelationshipType();

    @Value.Default
    default List<String> nonNegativeRelationshipTypes() {
        return List.of();
    }

    @Configuration.Ignore
    @Value.Derived
    default List<RelationshipType> superRelationshipTypes() {
        return Stream.concat(nonNegativeRelationshipTypes().stream(), relationshipTypes().stream())
            .map(RelationshipType::of)
            .collect(Collectors.toList());
    }

    @Configuration.GraphStoreValidationCheck
    @Value.Default
    default void validateRemainingRelType(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        validateTypeDoesNotExist(graphStore, remainingRelationshipType());
    }

    @Configuration.GraphStoreValidationCheck
    @Value.Default
    default void validateNonNegativeRelTypesExist(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        nonNegativeRelationshipTypes().forEach(relationshipType -> {
            if (!graphStore.hasRelationshipType(RelationshipType.of(relationshipType))) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Relationship type `%s` does not exist in the in-memory graph.",
                    relationshipType
                ));
            }
        });
    }

    @Configuration.GraphStoreValidationCheck
    @Value.Default
    default void validateHoldOutRelType(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        validateTypeDoesNotExist(graphStore, holdoutRelationshipType());
    }


    @Configuration.Ignore
    default void validateTypeDoesNotExist(GraphStore graphStore, RelationshipType holdoutRelationshipType) {
        if (graphStore.hasRelationshipType(holdoutRelationshipType)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Relationship type `%s` already exists in the in-memory graph.",
                holdoutRelationshipType.name()
            ));
        }
    }
}
