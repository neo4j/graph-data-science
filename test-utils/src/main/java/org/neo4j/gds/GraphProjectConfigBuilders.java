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
package org.neo4j.gds;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromCypherConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.progress.JobId;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PUBLIC, depluralize = true, deepImmutablesDetection = true)
public final class GraphProjectConfigBuilders {

    private GraphProjectConfigBuilders() {}

    /**
     * Factory method that defines the generation of {@link StoreConfigBuilder}.
     */
    @Builder.Factory
    public static GraphProjectFromStoreConfig storeConfig(
        Optional<String> userName,
        Optional<String> graphName,
        List<String> nodeLabels,
        List<String> relationshipTypes,
        List<NodeProjection> nodeProjections,
        List<RelationshipProjection> relationshipProjections,
        Map<String, NodeProjection> nodeProjectionsWithIdentifier,
        Map<String, RelationshipProjection> relationshipProjectionsWithIdentifier,
        List<PropertyMapping> nodeProperties,
        List<PropertyMapping> relationshipProperties,
        Optional<Integer> concurrency,
        Optional<JobId> jobId,
        Optional<Orientation> globalProjection,
        Optional<Aggregation> globalAggregation,
        Optional<Boolean> globalIndexInverse,
        Optional<Boolean> validateRelationships
    ) {
        // Node projections
        Map<String, NodeProjection> tempNP = new LinkedHashMap<>();
        nodeLabels.forEach(label -> tempNP.put(label, NodeProjection.of(label, PropertyMappings.of())));
        nodeProjections.forEach(np -> tempNP.put(np.label(), np));
        nodeProjectionsWithIdentifier.forEach(tempNP::put);

        if (tempNP.isEmpty()) {
            tempNP.put(ALL_NODES.name, NodeProjection.all());
        }

        // Relationship projections
        Map<String, RelationshipProjection> tempRP = new LinkedHashMap<>();
        Orientation orientation = globalProjection.orElse(Orientation.NATURAL);
        Aggregation aggregation = globalAggregation.orElse(Aggregation.DEFAULT);
        boolean indexInverse = globalIndexInverse.orElse(false);

        relationshipTypes.forEach(relType -> tempRP.put(
            relType,
            RelationshipProjection
                .builder()
                .type(relType)
                .orientation(orientation)
                .aggregation(aggregation)
                .indexInverse(indexInverse)
                .build()
        ));
        relationshipProjections.forEach(rp -> tempRP.put(rp.type(), rp));
        relationshipProjectionsWithIdentifier.forEach(tempRP::put);

        if (tempRP.isEmpty()) {
            tempRP.put(ALL_RELATIONSHIPS.name, RelationshipProjection.builder()
                .type("*")
                .orientation(orientation)
                .aggregation(aggregation)
                .indexInverse(indexInverse)
                .build());
        }

        PropertyMappings relationshipPropertyMappings = PropertyMappings.builder()
            .addAllMappings(relationshipProperties)
            .withDefaultAggregation(aggregation)
            .build();

        NodeProjections np = NodeProjections.of(tempNP.entrySet().stream().collect(Collectors.toMap(
            e -> NodeLabel.of(e.getKey()),
            Map.Entry::getValue
        )));

        RelationshipProjections rp = ImmutableRelationshipProjections.of(tempRP.entrySet().stream().collect(Collectors.toMap(
            e -> RelationshipType.of(e.getKey()),
            Map.Entry::getValue
        )));

        return ImmutableGraphProjectFromStoreConfig.builder()
            .username(userName.orElse(""))
            .graphName(graphName.orElse(""))
            .nodeProjections(np)
            .relationshipProjections(rp)
            .nodeProperties(PropertyMappings.of(nodeProperties))
            .relationshipProperties(relationshipPropertyMappings)
            .readConcurrency(concurrency.orElse(ConcurrencyConfig.DEFAULT_CONCURRENCY))
            .jobId(jobId.orElse(new JobId()))
            .validateRelationships(validateRelationships.orElse(false))
            .build()
            .withNormalizedPropertyMappings();
    }

    /**
     * Factory method that defines the generation of {@link CypherConfigBuilder}.
     */
    @Builder.Factory
    public static GraphProjectFromCypherConfig cypherConfig(
        Optional<String> userName,
        Optional<String> graphName,
        Optional<String> nodeQuery,
        Optional<String> relationshipQuery,
        Optional<Integer> concurrency,
        Optional<JobId> jobId,
        Optional<Boolean> validateRelationships,
        Optional<Map<String, Object>> parameters
    ) {

        return ImmutableGraphProjectFromCypherConfig.builder()
            .username(userName.orElse(""))
            .graphName(graphName.orElse(""))
            .nodeQuery(nodeQuery.orElse(ALL_NODES_QUERY))
            .relationshipQuery(relationshipQuery.orElse(ALL_RELATIONSHIPS_QUERY))
            .readConcurrency(concurrency.orElse(ConcurrencyConfig.DEFAULT_CONCURRENCY))
            .validateRelationships(validateRelationships.orElse(true))
            .parameters(parameters.orElse(Collections.emptyMap()))
            .jobId(jobId.orElse(new JobId()))
            .build();
    }
}
