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

import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

public interface GraphProjectConfigSupport {

    default GraphProjectFromStoreConfig emptyWithNameNative(String userName, String graphName, List<String> nodeProperties
    ) {
        return withNameAndRelationshipProjections(
            userName,
            graphName,
            RelationshipProjections.ALL,
            nodeProperties
        );
    }

    default GraphProjectFromStoreConfig withNameAndRelationshipProjections(
        String userName,
        String graphName,
        RelationshipProjections rels,
        List<String> nodeProperties
    ) {
        var propertyMappings = nodeProperties
            .stream()
            .map(PropertyMapping::of)
            .collect(Collectors.toList());
        return ImmutableGraphProjectFromStoreConfig.of(
            userName,
            graphName,
            NodeProjections.create(singletonMap(
                ALL_NODES,
                ImmutableNodeProjection.of(PROJECT_ALL, PropertyMappings.of(propertyMappings))
            )),
            rels
        );
    }

    default GraphProjectFromStoreConfig withNameAndNodeProjections(
        String userName,
        String graphName,
        NodeProjections nodes
    ) {
        return ImmutableGraphProjectFromStoreConfig.of(
            userName,
            graphName,
            nodes,
            RelationshipProjections.ALL
        );
    }

    default GraphProjectFromStoreConfig withNameAndProjections(
        String userName,
        String graphName,
        NodeProjections nodes,
        RelationshipProjections rels
    ) {
        return ImmutableGraphProjectFromStoreConfig.of(
            userName,
            graphName,
            nodes,
            rels
        );
    }

    default GraphProjectFromCypherConfig emptyWithNameCypher(String userName, String graphName, List<String> nodeProperties
    ) {
        return withNameAndRelationshipQuery(
            userName,
            graphName,
            ALL_RELATIONSHIPS_QUERY,
            nodeProperties
        );
    }

    default GraphProjectFromCypherConfig withNameAndRelationshipQuery(
        String userName,
        String graphName,
        String relationshipQuery,
        List<String> nodeProperties
    ) {
        String propertyPart = nodeProperties.stream().map(p -> "n." + p + " AS " + p).collect(Collectors.joining(", "));
        String nodesQuery = nodeProperties.isEmpty() ? ALL_NODES_QUERY : ALL_NODES_QUERY + ", " + propertyPart;
        return GraphProjectFromCypherConfig.of(
            userName,
            graphName,
            nodesQuery,
            relationshipQuery,
            CypherMapWrapper.empty()
        );
    }
}
