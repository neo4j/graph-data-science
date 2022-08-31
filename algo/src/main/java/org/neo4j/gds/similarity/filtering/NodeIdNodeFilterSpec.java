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
package org.neo4j.gds.similarity.filtering;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;

import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeIdNodeFilterSpec implements NodeFilterSpec {

    private final Set<Long> nodeIds;

    NodeIdNodeFilterSpec(Set<Long> nodeIds) {
        this.nodeIds = nodeIds;
    }

    @Override
    public NodeFilter toNodeFilter(IdMap idMap) {
        return NodeIdNodeFilter.create(nodeIds, idMap);
    }

    @Override
    public String render() {
        return "NodeFilter" + nodeIds.toString();
    }

    @Override
    public void validate(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        String nodeFilterType
    ) throws IllegalArgumentException {
        var existingNodeIds = graphStore.nodes();

        var missingNodes = nodeIds
            .stream()
            .filter(Predicate.not(existingNodeIds::contains))
            .map(String::valueOf)
            .collect(Collectors.joining(","));

        if (!missingNodes.isBlank()) {
            var errorMessage = formatWithLocale(
                "Invalid configuration value `%s`, the following nodes are missing from the graph: [%s]",
                nodeFilterType,
                String.join(",", missingNodes)
            );

            throw new IllegalArgumentException(errorMessage);
        }
    }
}
