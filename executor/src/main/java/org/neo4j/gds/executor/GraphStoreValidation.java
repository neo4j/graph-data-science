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
package org.neo4j.gds.executor;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.SourceNodeConfig;
import org.neo4j.gds.config.SourceNodesConfig;
import org.neo4j.gds.config.TargetNodeConfig;
import org.neo4j.gds.config.TargetNodePropertyConfig;
import org.neo4j.gds.config.TargetNodesConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GraphStoreValidation {

    public static void validate(GraphStore graphStore, AlgoBaseConfig config) {
        Collection<NodeLabel> filterLabels = config.nodeLabelIdentifiers(graphStore);

        if (config instanceof SourceNodesConfig) {
            validateSourceNodes(graphStore, (SourceNodesConfig) config, filterLabels);
        }
        if (config instanceof SourceNodeConfig) {
            validateSourceNode(graphStore, (SourceNodeConfig) config, filterLabels);
        }
        if (config instanceof TargetNodeConfig) {
            validateTargetNode(graphStore, (TargetNodeConfig) config, filterLabels);
        }
        if (config instanceof TargetNodesConfig) {
            validateTargetNodes(graphStore, (TargetNodesConfig) config, filterLabels);
        }
        if (config instanceof TargetNodePropertyConfig) {
            validateTargetNodeProperty(graphStore, (TargetNodePropertyConfig) config, filterLabels);
        }
    }

    private static void validateSourceNode(
        GraphStore graphStore,
        SourceNodeConfig config,
        Collection<NodeLabel> filterLabels
    ) {
        var sourceNodeId = config.sourceNode();

        if (labelFilteredGraphContainsNode(filterLabels, graphStore.nodes(), sourceNodeId)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Source node does not exist in the in-memory graph%s: `%d`",
                nodeLabelFilterDescription(filterLabels, graphStore),
                sourceNodeId
            ));
        }
    }

    private static void validateSourceNodes(GraphStore graphStore, SourceNodesConfig config, Collection<NodeLabel> filteredNodeLabels) {
        validateNodes(graphStore, config.sourceNodes(), filteredNodeLabels, "Source");
    }

    private static void validateTargetNode(
        GraphStore graphStore,
        TargetNodeConfig config,
        Collection<NodeLabel> filterLabels
    ) {
        var targetNodeId = config.targetNode();

        if (labelFilteredGraphContainsNode(filterLabels, graphStore.nodes(), targetNodeId)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Target node does not exist in the in-memory graph%s: `%d`",
                nodeLabelFilterDescription(filterLabels, graphStore),
                targetNodeId
            ));
        }
    }

    private static void validateTargetNodes(
        GraphStore graphStore,
        TargetNodesConfig config,
        Collection<NodeLabel> filteredNodeLabels
    ) {
        validateNodes(graphStore, config.targetNodes(), filteredNodeLabels, "Target");
    }

    private static void validateNodes(GraphStore graphStore, Collection<Long> nodesToValidate, Collection<NodeLabel> filteredNodeLabels, String nodeDescription) {
        if (!nodesToValidate.isEmpty()) {
            var missingNodes = nodesToValidate
                .stream()
                .filter(targetNode -> labelFilteredGraphContainsNode(filteredNodeLabels, graphStore.nodes(), targetNode))
                .map(Object::toString)
                .collect(Collectors.toList());

            if (!missingNodes.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "%s nodes do not exist in the in-memory graph%s: %s",
                    nodeDescription,
                    nodeLabelFilterDescription(filteredNodeLabels, graphStore),
                    StringJoining.join(missingNodes)
                ));
            }
        }
    }

    private static String nodeLabelFilterDescription(Collection<NodeLabel> filteredNodeLabels, GraphStore graphStore) {
        return filteredNodeLabels.containsAll(graphStore.nodeLabels())
            ? ""
            : " for the labels " + StringJoining.join(filteredNodeLabels.stream().map(NodeLabel::name));
    }

    private static boolean labelFilteredGraphContainsNode(
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

    private static void validateTargetNodeProperty(
        GraphStore graphStore,
        TargetNodePropertyConfig config,
        Collection<NodeLabel> filterLabels
    ) {
        var targetProperty = config.targetProperty();
        if (targetProperty != null && !graphStore.hasNodeProperty(filterLabels, targetProperty)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Target property `%s` not found in graph with node properties: %s",
                targetProperty,
                graphStore.nodePropertyKeys()
            ));
        }
    }

    private GraphStoreValidation() {}
}
