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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ConfigNodesValidations {

    private ConfigNodesValidations() {}

    /**
     * When parsing nodes in a config we can validate the node ids are non-negative.
     * @param nodes collection of nodes to validate
     * @param parameterKey the parameter key under which the user submitted these nodes
     */
    static void nodesNotNegative(Collection<Long> nodes, String parameterKey) {
        var negativeNodes = nodes.stream().filter(n -> n < 0).collect(Collectors.toList());
        if (negativeNodes.isEmpty()) return;
        throw new IllegalArgumentException(formatWithLocale(
            "Negative node ids are not supported for the field `%s`. Negative node ids: %s",
            parameterKey, negativeNodes
        ));
    }

    /**
     * Once we have a graph store and the filter labels we can validate that the nodes exist in the graph.
     * @param graphStore
     * @param nodes collection of nodes to validate
     * @param filteredNodeLabels
     * @param parameterKey the parameter key under which the user submitted these nodes
     */
    static void nodesExistInGraph(
        GraphStore graphStore,
        Collection<NodeLabel> filteredNodeLabels,
        Collection<Long> nodes,
        String parameterKey
    ) {
        var missingNodes = nodes
            .stream()
            .filter(targetNode -> labelFilteredGraphNotContainsNode(
                filteredNodeLabels,
                graphStore.nodes(),
                targetNode
            ))
            .map(Object::toString)
            .collect(Collectors.toList());

        if (!missingNodes.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "%s nodes do not exist in the in-memory graph%s: %s",
                parameterKey,
                nodeLabelFilterDescription(filteredNodeLabels, graphStore),
                missingNodes
            ));
        }
    }

    public static void validateNodePropertyExists(
        GraphStore graphStore,
        Collection<NodeLabel> nodeLabels,
        String configKey,
        @Nullable String propertyName
    ) {
        if (graphStore.hasNodeProperty(nodeLabels, propertyName)) return;
        throw new IllegalArgumentException(formatWithLocale(
            "%s `%s` not found in graph with node properties: %s",
            configKey,
            propertyName,
            graphStore.nodePropertyKeys().stream().sorted().collect(Collectors.toList())
        ));
    }

    static boolean labelFilteredGraphNotContainsNode(
        Collection<NodeLabel> filteredNodeLabels,
        IdMap idMap,
        long neoNodeId
    ) {
        var internalNodeId = idMap.safeToMappedNodeId(neoNodeId);
        return internalNodeId == IdMap.NOT_FOUND || idMap
            .nodeLabels(internalNodeId)
            .stream()
            .noneMatch(filteredNodeLabels::contains);
    }

    static String nodeLabelFilterDescription(Collection<NodeLabel> filteredNodeLabels, GraphStore graphStore) {
        return filteredNodeLabels.containsAll(graphStore.nodeLabels())
            ? ""
            : " for the labels " + StringJoining.join(filteredNodeLabels.stream().map(NodeLabel::name));
    }
}
