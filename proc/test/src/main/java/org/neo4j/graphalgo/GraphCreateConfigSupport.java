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
package org.neo4j.graphalgo;

import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;

import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

public interface GraphCreateConfigSupport {

    default GraphCreateFromStoreConfig emptyWithNameNative(String userName, String graphName) {
        return withNameAndRelationshipProjections(
            userName,
            graphName,
            RelationshipProjections.all()
        );
    }

    default GraphCreateFromStoreConfig withNameAndRelationshipProjections(
        String userName,
        String graphName,
        RelationshipProjections rels
    ) {
        return ImmutableGraphCreateFromStoreConfig.of(
            userName,
            graphName,
            NodeProjections.all(),
            rels
        );
    }

    default GraphCreateFromStoreConfig withNameAndNodeProjections(
        String userName,
        String graphName,
        NodeProjections nodes
    ) {
        return ImmutableGraphCreateFromStoreConfig.of(
            userName,
            graphName,
            nodes,
            RelationshipProjections.all()
        );
    }

    default GraphCreateFromCypherConfig emptyWithNameCypher(String userName, String graphName) {
        return withNameAndRelationshipQuery(
            userName,
            graphName,
            ALL_RELATIONSHIPS_QUERY
        );
    }

    default GraphCreateFromCypherConfig withNameAndRelationshipQuery(
        String userName,
        String graphName,
        String relationshipQuery
    ) {
        return GraphCreateFromCypherConfig.of(
            userName,
            graphName,
            ALL_NODES_QUERY,
            relationshipQuery,
            CypherMapWrapper.empty()
        );
    }
}
