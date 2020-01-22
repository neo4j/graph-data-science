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
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromCypherConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromStoreConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

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
        Optional<Boolean> loadAnyLabel,
        Optional<Boolean> loadAnyRelationshipType,
        Optional<Projection> globalProjection
    ) {
        // Node projections
        Map<String, NodeProjection> tempNP = new HashMap<>();
        nodeLabels.forEach(label -> tempNP.put(label, NodeProjection.of(label, PropertyMappings.of())));
        nodeProjections.forEach(np -> tempNP.put(np.label().orElse("*"), np));
        nodeProjectionsWithIdentifier.forEach(tempNP::put);

        if (tempNP.isEmpty() && loadAnyLabel.orElse(null) != null) {
            tempNP.put("*", NodeProjection.empty());
        }

        // Relationship projections
        Map<String, RelationshipProjection> tempRP = new HashMap<>();
        relationshipTypes.forEach(relType -> tempRP.put(
            relType,
            RelationshipProjection.of(relType, Projection.NATURAL, DeduplicationStrategy.DEFAULT)
        ));
        relationshipProjections.forEach(rp -> tempRP.put(rp.type().orElse("*"), rp));
        relationshipProjectionsWithIdentifier.forEach(tempRP::put);

        if (tempRP.isEmpty() && loadAnyRelationshipType.orElse(false)) {
            tempRP.put("*", RelationshipProjection.empty());
        }

        NodeProjections np = NodeProjections.of(tempNP.entrySet().stream().collect(Collectors.toMap(
            e -> ElementIdentifier.of(e.getKey()),
            Map.Entry::getValue
        )));

        RelationshipProjections rp = RelationshipProjections.of(tempRP.entrySet().stream().collect(Collectors.toMap(
            e -> ElementIdentifier.of(e.getKey()),
            e -> globalProjection.isPresent()
                ? RelationshipProjection.copyOf(e.getValue()).withProjection(globalProjection.get())
                : e.getValue()
        )));

        return ImmutableGraphCreateFromStoreConfig.builder()
            .username(userName.orElse(""))
            .graphName(graphName.orElse(""))
            .nodeProjection(np)
            .relationshipProjection(rp)
            .nodeProperties(PropertyMappings.of(nodeProperties))
            .relationshipProperties(PropertyMappings.of(relationshipProperties))
            .concurrency(concurrency.orElse(Pools.DEFAULT_CONCURRENCY))
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
        Optional<Boolean> loadAnyLabel,
        Optional<Boolean> loadAnyRelationshipType,
        Optional<Integer> concurrency
    ) {
        if (!nodeQuery.isPresent() && !loadAnyLabel.orElse(false)) {
            throw new IllegalArgumentException("Missing nodeQuery or loadAnyLabel(true).");
        }

        if (!relationshipQuery.isPresent() && !loadAnyRelationshipType.orElse(false)) {
            throw new IllegalArgumentException("Missing relationshipQuery or loadAnyRelationshipType(true).");
        }

        return ImmutableGraphCreateFromCypherConfig.builder()
            .username(userName.orElse(""))
            .graphName(graphName.orElse(""))
            .nodeQuery(nodeQuery.orElse(ALL_NODES_QUERY))
            .relationshipQuery(relationshipQuery.orElse(ALL_RELATIONSHIPS_QUERY))
            .nodeProperties(PropertyMappings.of(nodeProperties))
            .relationshipProperties(PropertyMappings.of(relationshipProperties))
            .concurrency(concurrency.orElse(Pools.DEFAULT_CONCURRENCY))
            .build();
    }

}
