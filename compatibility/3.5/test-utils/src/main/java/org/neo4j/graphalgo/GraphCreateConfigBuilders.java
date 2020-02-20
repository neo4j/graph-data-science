/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.Aggregation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PUBLIC, depluralize = true, deepImmutablesDetection = true)
final class GraphCreateConfigBuilders {

    private GraphCreateConfigBuilders() { }

    /**
     * Factory method that defines the generation of {@link StoreConfigBuilder}.
     */
    @Builder.Factory
    static GraphCreateFromStoreConfig storeConfig(
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
        @Builder.Switch(defaultName = "PROJECTION") AnyLabel anyLabel,
        @Builder.Switch(defaultName = "PROJECTION") AnyRelationshipType anyRelationshipType,
        Optional<Orientation> globalProjection,
        Optional<Aggregation> globalAggregation
    ) {
        // Node projections
        Map<String, NodeProjection> tempNP = new LinkedHashMap<>();
        nodeLabels.forEach(label -> tempNP.put(label, NodeProjection.of(label, PropertyMappings.of())));
        nodeProjections.forEach(np -> tempNP.put(np.label(), np));
        nodeProjectionsWithIdentifier.forEach(tempNP::put);

        if (tempNP.isEmpty() && anyLabel == AnyLabel.LOAD) {
            tempNP.put("*", NodeProjection.all());
        }

        // Relationship projections
        Map<String, RelationshipProjection> tempRP = new LinkedHashMap<>();
        Orientation orientation = globalProjection.orElse(Orientation.NATURAL);
        Aggregation aggregation = globalAggregation.orElse(Aggregation.DEFAULT);

        relationshipTypes.forEach(relType -> tempRP.put(
            relType,
            RelationshipProjection.of(relType, orientation, aggregation)
        ));
        relationshipProjections.forEach(rp -> tempRP.put(rp.type(), rp));
        relationshipProjectionsWithIdentifier.forEach(tempRP::put);

        if (tempRP.isEmpty() && anyRelationshipType == AnyRelationshipType.LOAD) {
            tempRP.put("*", RelationshipProjection.builder()
                .type("*")
                .orientation(orientation)
                .aggregation(aggregation)
                .build());
        }

        PropertyMappings relationshipPropertyMappings = PropertyMappings.builder()
            .addAllMappings(relationshipProperties)
            .withDefaultAggregation(aggregation)
            .build();

        NodeProjections np = NodeProjections.of(tempNP.entrySet().stream().collect(Collectors.toMap(
            e -> ElementIdentifier.of(e.getKey()),
            Map.Entry::getValue
        )));

        RelationshipProjections rp = RelationshipProjections.of(tempRP.entrySet().stream().collect(Collectors.toMap(
            e -> ElementIdentifier.of(e.getKey()),
            Map.Entry::getValue
        )));

        return ImmutableGraphCreateFromStoreConfig.builder()
            .username(userName.orElse(""))
            .graphName(graphName.orElse(""))
            .nodeProjections(np)
            .relationshipProjections(rp)
            .nodeProperties(PropertyMappings.of(nodeProperties))
            .relationshipProperties(relationshipPropertyMappings)
            .concurrency(concurrency.orElse(AlgoBaseConfig.DEFAULT_CONCURRENCY))
            .build()
            .withNormalizedPropertyMappings();
    }

    /**
     * Factory method that defines the generation of {@link StoreConfigBuilder}.
     */
    @Builder.Factory
    static GraphCreateFromCypherConfig cypherConfig(
        Optional<String> userName,
        Optional<String> graphName,
        Optional<String> nodeQuery,
        Optional<String> relationshipQuery,
        List<PropertyMapping> nodeProperties,
        List<PropertyMapping> relationshipProperties,
        @Builder.Switch(defaultName = "PROJECTION") AnyLabel anyLabel,
        @Builder.Switch(defaultName = "PROJECTION") AnyRelationshipType anyRelationshipType,
        Optional<Integer> concurrency,
        Optional<Aggregation> globalAggregation
    ) {
        if (!(nodeQuery.isPresent() || anyLabel == AnyLabel.LOAD)) {
            throw new IllegalArgumentException("Missing nodeQuery or loadAnyLabel().");
        }

        if (!(relationshipQuery.isPresent() || anyRelationshipType == AnyRelationshipType.LOAD)) {
            throw new IllegalArgumentException("Missing relationshipQuery or loadAnyRelationshipType().");
        }

        // TODO: This is a temporary hack to allow setting a global aggregation for Cypher
        //       loading in tests. Remove this as soon as projections and queries can be specified in conjunction.
        RelationshipProjections relationshipProjections;
        PropertyMappings relationshipPropertyMappings;
        if (globalAggregation.isPresent()) {
            Aggregation aggregation = globalAggregation.get();
            relationshipProjections = RelationshipProjections.builder()
                .putProjection(
                    PROJECT_ALL,
                    RelationshipProjection.of("*", Orientation.NATURAL, aggregation)
                )
                .build();
            relationshipPropertyMappings = PropertyMappings.builder()
                .addAllMappings(relationshipProperties)
                .withDefaultAggregation(aggregation)
                .build();
        } else {
            relationshipProjections = RelationshipProjections.empty();
            relationshipPropertyMappings = PropertyMappings.of(relationshipProperties);
        }

        return ImmutableGraphCreateFromCypherConfig.builder()
            .username(userName.orElse(""))
            .graphName(graphName.orElse(""))
            .nodeQuery(nodeQuery.orElse(ALL_NODES_QUERY))
            .relationshipQuery(relationshipQuery.orElse(ALL_RELATIONSHIPS_QUERY))
            .relationshipProjections(relationshipProjections)
            .nodeProperties(PropertyMappings.of(nodeProperties))
            .relationshipProperties(relationshipPropertyMappings)
            .concurrency(concurrency.orElse(AlgoBaseConfig.DEFAULT_CONCURRENCY))
            .build();
    }

    enum AnyLabel {
        PROJECTION, LOAD
    }

    enum AnyRelationshipType {
        PROJECTION, LOAD
    }
}
