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
package org.neo4j.gds.config;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;

import java.util.Collection;
import java.util.Set;

import static org.neo4j.gds.config.ConfigNodesValidations.nodesExistInGraph;
import static org.neo4j.gds.config.ConfigNodesValidations.nodesNotNegative;
import static org.neo4j.gds.config.NodeIdParser.parseToSingleNodeId;

public interface SourceNodeConfig {

    String SOURCE_NODE_KEY = "sourceNode";

    @Configuration.ConvertWith(method = "org.neo4j.gds.config.SourceNodeConfig#parseSourceNode")
    long sourceNode();

    static long parseSourceNode(Object input) {
        var node = parseToSingleNodeId(input, SOURCE_NODE_KEY);
        nodesNotNegative(Set.of(node), SOURCE_NODE_KEY);
        return node;
    }

    @Configuration.GraphStoreValidationCheck
    default void validateSourceNode(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        nodesExistInGraph(graphStore, selectedLabels, Set.of(sourceNode()), SOURCE_NODE_KEY);
    }
}
