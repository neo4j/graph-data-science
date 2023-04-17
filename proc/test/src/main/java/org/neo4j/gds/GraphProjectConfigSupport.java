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

import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;

import static java.util.Collections.singletonMap;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.NodeLabel.ALL_NODES;

public interface GraphProjectConfigSupport {

    default GraphProjectFromStoreConfig emptyWithNameNative(String userName, String graphName) {
        return withNameAndRelationshipProjections(
            userName,
            graphName,
            RelationshipProjections.ALL
        );
    }

    default GraphProjectFromStoreConfig withNameAndRelationshipProjections(
        String userName,
        String graphName,
        RelationshipProjections rels
    ) {
        return ImmutableGraphProjectFromStoreConfig.of(
            userName,
            graphName,
            NodeProjections.create(singletonMap(
                ALL_NODES,
                ImmutableNodeProjection.of(PROJECT_ALL, ImmutablePropertyMappings.of())
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
}
