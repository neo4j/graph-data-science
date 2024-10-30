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
package org.neo4j.gds.projection;

import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.Configuration.ConvertWith;
import org.neo4j.gds.annotation.Configuration.Key;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface GraphProjectFromStoreConfig extends GraphProjectConfig {

    @Configuration.Ignore
    default Map<String, Object> asProcedureResultConfigurationField() {
        return cleansed(toMap(), outputFieldDenylist());
    }

    String NODE_PROJECTION_KEY = "nodeProjection";
    String RELATIONSHIP_PROJECTION_KEY = "relationshipProjection";
    String NODE_PROPERTIES_KEY = "nodeProperties";
    String RELATIONSHIP_PROPERTIES_KEY = "relationshipProperties";

    @Key(NODE_PROJECTION_KEY)
    @ConvertWith(method = "org.neo4j.gds.NodeProjections#fromObject")
    @Configuration.ToMapValue("org.neo4j.gds.NodeProjections#toObject")
    NodeProjections nodeProjections();

    @Key(RELATIONSHIP_PROJECTION_KEY)
    @ConvertWith(method = "org.neo4j.gds.RelationshipProjections#fromObject")
    @Configuration.ToMapValue("org.neo4j.gds.RelationshipProjections#toObject")
    RelationshipProjections relationshipProjections();

    @Configuration.ConvertWith(method = "org.neo4j.gds.PropertyMappings#fromObject")
    @Configuration.ToMapValue("org.neo4j.gds.PropertyMappings#toObject")
    default PropertyMappings nodeProperties() {
        return PropertyMappings.of();
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.PropertyMappings#fromObject")
    @Configuration.ToMapValue("org.neo4j.gds.PropertyMappings#toObject")
    default PropertyMappings relationshipProperties() {
        return PropertyMappings.of();
    }

    @Configuration.Check
    default void validateProjectionsAreNotEmpty() {
        if (nodeProjections().isEmpty()) {
            throw new IllegalArgumentException(
                "The parameter 'nodeProjections' should not be empty. Use '*' to load all nodes."
            );
        }

        if (relationshipProjections().isEmpty()) {
            throw new IllegalArgumentException(
                "The parameter 'relationshipProjections' should not be empty. Use '*' to load all Relationships."
            );
        }
    }

    @Configuration.Check
    default GraphProjectFromStoreConfig withNormalizedPropertyMappings() {
        PropertyMappings nodeProperties = nodeProperties();
        PropertyMappings relationshipProperties = relationshipProperties();

        if (!nodeProperties.hasMappings() && !relationshipProperties.hasMappings()) {
            return this;
        }

        relationshipProjections().projections().values().forEach(relationshipProjection -> {
            if (relationshipProjection.properties().mappings().size() > 1) {
                throw new IllegalArgumentException(
                    "Implicit graph loading does not allow loading multiple relationship properties per relationship type");
            }
        });

        verifyProperties(
            nodeProperties.stream().map(PropertyMapping::propertyKey).collect(Collectors.toSet()),
            nodeProjections().allProperties(),
            "node"
        );

        verifyProperties(
            relationshipProperties.stream().map(PropertyMapping::propertyKey).collect(Collectors.toSet()),
            relationshipProjections().allProperties(),
            "relationship"
        );

        var normalizedNodeProjections = nodeProjections().addPropertyMappings(nodeProperties);
        var normalizedRelationshipProjections = relationshipProjections().addPropertyMappings(relationshipProperties);

        // We have to trigger the validation of the aggregation here, since the projections might have been updated.
        normalizedRelationshipProjections.projections().values().forEach(RelationshipProjection::checkAggregation);

        return GraphProjectFromStoreConfigImpl.Builder
            .from(this)
            .nodeProjections(normalizedNodeProjections)
            .nodeProperties(PropertyMappings.of())
            .relationshipProjections(normalizedRelationshipProjections)
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
            throw new IllegalArgumentException(formatWithLocale(
                "Incompatible %s projection and %s property specification. Both specify properties named %s",
                type, type, propertyIntersection
            ));
        }
    }

    @Configuration.Ignore
    default Set<String> outputFieldDenylist() {
        return Set.of(NODE_COUNT_KEY, RELATIONSHIP_COUNT_KEY);
    }

    static GraphProjectFromStoreConfig of(
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

        return GraphProjectFromStoreConfigImpl.of(
            userName,
            graphName,
            config
        );
    }

    static GraphProjectFromStoreConfig all(String userName, String graphName) {
        return GraphProjectFromStoreConfigImpl.builder()
            .username(userName)
            .graphName(graphName)
            .nodeProjections(NodeProjections.all())
            .relationshipProjections(RelationshipProjections.ALL)
            .build();
    }

    static GraphProjectFromStoreConfig fromProcedureConfig(String username, CypherMapWrapper config) {
        if (!config.containsKey(NODE_PROJECTION_KEY)) {
            config = config.withEntry(NODE_PROJECTION_KEY, NodeProjections.all());
        }
        if (!config.containsKey(RELATIONSHIP_PROJECTION_KEY)) {
            config = config.withEntry(RELATIONSHIP_PROJECTION_KEY, RelationshipProjections.ALL);
        }

        return GraphProjectFromStoreConfigImpl.of(
            username,
            IMPLICIT_GRAPH_NAME,
            config
        );
    }
}
