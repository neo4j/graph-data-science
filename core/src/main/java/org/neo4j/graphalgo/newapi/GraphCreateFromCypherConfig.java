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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;

import java.util.List;
import java.util.Optional;

import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

@ValueClass
@Configuration("GraphCreateFromCypherConfigImpl")
public interface GraphCreateFromCypherConfig extends GraphCreateConfig {

    String NODE_QUERY_KEY = "nodeQuery";
    String RELATIONSHIP_QUERY_KEY = "relationshipQuery";
    String ALL_NODES_QUERY = "MATCH (n) RETURN id(n) AS id";
    String ALL_RELATIONSHIPS_QUERY = "MATCH (a)-->(b) RETURN id(a) AS source, id(b) AS target";

    @Override
    @Configuration.Ignore
    default Class<? extends GraphFactory> getGraphImpl() {
        return CypherGraphFactory.class;
    }

    @Override
    @Value.Default
    default NodeProjections nodeProjection() {
        return NodeProjections.of();
    }

    @Override
    @Value.Default
    default RelationshipProjections relationshipProjection() {
        return RelationshipProjections.of();
    }

    @Configuration.ConvertWith("org.apache.commons.lang3.StringUtils#trimToNull")
    String nodeQuery();

    @Configuration.ConvertWith("org.apache.commons.lang3.StringUtils#trimToNull")
    String relationshipQuery();

    @Configuration.Ignore
    default GraphCreateFromCypherConfig inferProjections(GraphDimensions dimensions) {
        NodeProjections nodeProjections = NodeProjections.builder()
            .putProjection(
                PROJECT_ALL,
                NodeProjection
                    .builder()
                    .label(PROJECT_ALL.name)
                    .addPropertyMappings(PropertyMappings.of(dimensions.nodeProperties()))
                    .build()
            ).build();

        PropertyMappings relationshipPropertyMappings = PropertyMappings.of(dimensions.relationshipProperties());

        RelationshipProjections.Builder relProjectionBuilder = RelationshipProjections.builder();
        dimensions.relationshipTypeMappings().stream().forEach(typeMapping -> {
            String relationshipType = typeMapping.typeName().isEmpty() ? PROJECT_ALL.name : typeMapping.typeName();
            relProjectionBuilder.putProjection(
                ElementIdentifier.of(relationshipType),
                RelationshipProjection
                    .builder()
                    .type(relationshipType)
                    .addPropertyMappings(relationshipPropertyMappings)
                    .build()
            );
        });

        return ImmutableGraphCreateFromCypherConfig
            .builder()
            .from(this)
            .nodeProjection(nodeProjections)
            .relationshipProjection(relProjectionBuilder.build())
            .build();
    }

    static GraphCreateFromCypherConfig of(
        String userName,
        String graphName,
        String nodeQuery,
        String relationshipQuery,
        CypherMapWrapper config
    ) {
        assertNoProjections(config);

        if (nodeQuery != null) {
            config = config.withString(NODE_QUERY_KEY, nodeQuery);
        }
        if (relationshipQuery != null) {
            config = config.withString(RELATIONSHIP_QUERY_KEY, relationshipQuery);
        }
        return new GraphCreateFromCypherConfigImpl(
            graphName,
            userName,
            config
        );
    }

    @TestOnly
    static GraphCreateFromCypherConfig emptyWithName(String userName, String graphName) {
        return GraphCreateFromCypherConfig.of(
            userName,
            graphName,
            ALL_NODES_QUERY,
            ALL_RELATIONSHIPS_QUERY,
            CypherMapWrapper.empty()
        );
    }

    static GraphCreateFromCypherConfig fromProcedureConfig(String username, CypherMapWrapper config) {
        assertNoProjections(config);
        return new GraphCreateFromCypherConfigImpl(
            IMPLICIT_GRAPH_NAME,
            username,
            config
        );
    }

    static void assertNoProjections(CypherMapWrapper config) {
        if (config.containsKey(NODE_PROJECTION_KEY)) {
            throw new IllegalArgumentException(String.format("Invalid key: %s", NODE_PROJECTION_KEY));
        }
        if (config.containsKey(RELATIONSHIP_PROJECTION_KEY)) {
            throw new IllegalArgumentException(String.format("Invalid key: %s", RELATIONSHIP_PROJECTION_KEY));
        }
    }

    /**
     * Factory method that defines the generation of {@link CypherConfigBuilder}.
     * Use the builder to construct the input for that input.
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
