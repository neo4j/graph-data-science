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
package org.neo4j.graphalgo.newapi;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.Configuration.ConvertWith;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.utils.Pools;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@ValueClass
@Configuration("GraphCreateFromStoreConfigImpl")
public interface GraphCreateFromStoreConfig extends GraphCreateConfig {

    String NODE_PROJECTION_KEY = "nodeProjection";
    String RELATIONSHIP_PROJECTION_KEY = "relationshipProjection";

    @Override
    @ConvertWith("org.neo4j.graphalgo.AbstractNodeProjections#fromObject")
    NodeProjections nodeProjection();

    @Override
    @ConvertWith("org.neo4j.graphalgo.AbstractRelationshipProjections#fromObject")
    RelationshipProjections relationshipProjection();

    @Value.Check
    default GraphCreateFromStoreConfig withNormalizedPropertyMappings() {
        PropertyMappings nodeProperties = nodeProperties();
        PropertyMappings relationshipProperties = relationshipProperties();

        if (!nodeProperties.hasMappings() && !relationshipProperties.hasMappings()) {
            return this;
        }

        relationshipProjection().projections().values().forEach(relationshipProjection -> {
            if (relationshipProjection.properties().mappings().size() > 1) {
                throw new IllegalArgumentException(
                    "Implicit graph loading does not allow loading multiple relationship properties per relationship type");
            }
        });

        verifyProperties(
            nodeProperties.stream().map(PropertyMapping::propertyKey).collect(Collectors.toSet()),
            nodeProjection().allProperties(),
            "node"
        );

        verifyProperties(
            relationshipProperties.stream().map(PropertyMapping::propertyKey).collect(Collectors.toSet()),
            relationshipProjection().allProperties(),
            "relationship"
        );

        return ImmutableGraphCreateFromStoreConfig
            .builder()
            .from(this)
            .nodeProjection(nodeProjection().addPropertyMappings(nodeProperties))
            .nodeProperties(PropertyMappings.of())
            .relationshipProjection(relationshipProjection().addPropertyMappings(relationshipProperties))
            .relationshipProperties(PropertyMappings.of())
            .build();
    }

    @Configuration.Ignore
    default void verifyProperties(
        Set<String> propertiesFromMapping,
        Set<String> propertiesFromProjection,
        String type
    ) {
        Set<String> propertyIntersection = new HashSet<>(propertiesFromMapping);
        propertyIntersection.retainAll(propertiesFromProjection);

        if (!propertyIntersection.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "Incompatible %s projection and %s property specification. Both specify properties named %s",
                type, type, propertyIntersection
            ));
        }
    }

    static GraphCreateFromStoreConfig emptyWithName(String userName, String graphName) {
        return ImmutableGraphCreateFromStoreConfig.of(userName, graphName, NodeProjections.empty(), RelationshipProjections.empty());
    }

    static GraphCreateFromStoreConfig of(
        String userName,
        String graphName,
        Object nodeProjections,
        Object relationshipProjections,
        CypherMapWrapper config
    ) {
        if (nodeProjections != null) {
            config = config.withEntry(NODE_PROJECTION_KEY, nodeProjections);
        }
        if (relationshipProjections != null) {
            config = config.withEntry(RELATIONSHIP_PROJECTION_KEY, relationshipProjections);
        }

        return GraphCreateFromStoreConfigImpl.of(
            graphName,
            userName,
            config
        );
    }

    static GraphCreateFromStoreConfig all(String userName, String graphName) {
        return ImmutableGraphCreateFromStoreConfig.builder()
            .username(userName)
            .graphName(graphName)
            .nodeProjection(NodeProjections.empty())
            .relationshipProjection(RelationshipProjections.empty())
            .build();
    }

    static GraphCreateFromStoreConfig fromProcedureConfig(String username, CypherMapWrapper config) {
        if (!config.containsKey(NODE_PROJECTION_KEY)) {
            config = config.withEntry(NODE_PROJECTION_KEY, NodeProjections.empty());
        }
        if (!config.containsKey(RELATIONSHIP_PROJECTION_KEY)) {
            config = config.withEntry(RELATIONSHIP_PROJECTION_KEY, RelationshipProjections.empty());
        }

        return GraphCreateFromStoreConfigImpl.of(
            IMPLICIT_GRAPH_NAME,
            username,
            config
        );
    }

    /**
     * Factory method that defines the generation of {@link StoreConfigBuilder}.
     * Use the builder to construct the input for that input.
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
            .build();
    }
}
