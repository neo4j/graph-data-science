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

package org.neo4j.graphalgo.config;

import org.immutables.value.Value;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.loading.CypherFactory;

import java.util.Collections;
import java.util.Map;

import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

@ValueClass
@Configuration("GraphCreateFromCypherConfigImpl")
@SuppressWarnings("immutables:subtype")
public interface GraphCreateFromCypherConfig extends GraphCreateConfig {

    String NODE_QUERY_KEY = "nodeQuery";
    String RELATIONSHIP_QUERY_KEY = "relationshipQuery";
    String ALL_NODES_QUERY = "MATCH (n) RETURN id(n) AS id";
    String ALL_RELATIONSHIPS_QUERY = "MATCH (a)-->(b) RETURN id(a) AS source, id(b) AS target";

    @Override
    @Configuration.Ignore
    default Class<? extends GraphStoreFactory> getGraphImpl() {
        return CypherFactory.class;
    }

    @Override
    @Value.Default
    @Configuration.Key(NODE_PROJECTION_KEY)
    default NodeProjections nodeProjections() {
        return NodeProjections.of();
    }

    @Override
    @Value.Default
    @Configuration.Key(RELATIONSHIP_PROJECTION_KEY)
    default RelationshipProjections relationshipProjections() {
        return RelationshipProjections.of();
    }

    @Configuration.ConvertWith("org.apache.commons.lang3.StringUtils#trimToNull")
    String nodeQuery();

    @Configuration.ConvertWith("org.apache.commons.lang3.StringUtils#trimToNull")
    String relationshipQuery();

    @Value.Default
    default Map<String, Object> parameters() {
        return Collections.emptyMap();
    }

    @Override
    @Value.Default
    @Value.Parameter(false)
    default boolean throwOnUnresolvedRelationships() {
        return true;
    }

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
        dimensions.relationshipProjectionMappings().stream().forEach(typeMapping -> {
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
            .nodeProjections(nodeProjections)
            .relationshipProjections(relProjectionBuilder.build())
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
}
