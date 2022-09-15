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

import static org.neo4j.gds.config.ConfigNodesValidations.labelFilteredGraphNotContainsNode;
import static org.neo4j.gds.config.ConfigNodesValidations.nodeLabelFilterDescription;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface SourceNodeConfig extends NodeConfig {

    String SOURCE_NODE_KEY = "sourceNode";

    @Configuration.ConvertWith(method = "org.neo4j.gds.config.SourceNodeConfig#parseSourceNodeId")
    long sourceNode();

    static long parseSourceNodeId(Object input) {
        return NodeConfig.parseNodeId(input, SOURCE_NODE_KEY);
    }

    @Configuration.GraphStoreValidationCheck
    default void validateSourceNode(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        var sourceNodeId = sourceNode();

        if (labelFilteredGraphNotContainsNode(selectedLabels, graphStore.nodes(), sourceNodeId)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Source node does not exist in the in-memory graph%s: `%d`",
                nodeLabelFilterDescription(selectedLabels, graphStore),
                sourceNodeId
            ));
        }
    }
}
