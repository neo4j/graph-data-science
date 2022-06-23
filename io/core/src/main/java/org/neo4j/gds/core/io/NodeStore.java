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
package org.neo4j.gds.core.io;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeIntArray;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;

public class NodeStore {

    private static final String[] EMPTY_LABELS = new String[0];

    final long nodeCount;

    private final HugeIntArray labelCounts;

    final IdMap idMap;

    final Map<String, Map<String, NodePropertyValues>> nodeProperties;
    final Map<String, LongFunction<Object>> additionalProperties;

    private final Set<NodeLabel> availableNodeLabels;

    private final boolean hasLabels;

    private NodeStore(
        long nodeCount,
        HugeIntArray labelCounts,
        IdMap idMap,
        boolean hasLabels,
        Map<String, Map<String, NodePropertyValues>> nodeProperties,
        Map<String, LongFunction<Object>> additionalProperties
    ) {
        this.nodeCount = nodeCount;
        this.labelCounts = labelCounts;
        this.idMap = idMap;
        this.nodeProperties = nodeProperties;
        this.hasLabels = hasLabels;
        this.availableNodeLabels = idMap.availableNodeLabels();
        this.additionalProperties = additionalProperties;
    }

    boolean hasLabels() {
        return hasLabels;
    }

    boolean hasProperties() {
        return nodeProperties != null;
    }

    int labelCount() {
        return !hasLabels() ? 0 : idMap.availableNodeLabels().size();
    }

    int propertyCount() {
        if (nodeProperties == null) {
            return 0;
        } else {
            return nodeProperties.values().stream().mapToInt(Map::size).sum();
        }
    }

    String[] labels(long nodeId) {
        int labelCount = labelCounts.get(nodeId);
        if (labelCount == 0) {
            return EMPTY_LABELS;
        }
        String[] labels = new String[labelCount];

        int i = 0;
        for (var nodeLabel : availableNodeLabels) {
            if (idMap.hasLabel(nodeId, nodeLabel)) {
                labels[i++] = nodeLabel.name;
            }
        }

        return labels;
    }

    static NodeStore of(
        GraphStore graphStore,
        Map<String, LongFunction<Object>> additionalProperties
    ) {
        HugeIntArray labelCounts = null;

        var nodeLabels = graphStore.nodes();
        var nodeProperties = new HashMap<String, Map<String, NodePropertyValues>>();

        boolean hasNodeLabels = !graphStore.schema().nodeSchema().containsOnlyAllNodesLabel();
        if (hasNodeLabels) {
            labelCounts = HugeIntArray.newArray(graphStore.nodeCount());
            labelCounts.setAll(i -> {
                int labelCount = 0;
                for (var nodeLabel : nodeLabels.availableNodeLabels()) {
                    if (nodeLabels.hasLabel(i, nodeLabel)) {
                        labelCount++;
                    }
                }
                return labelCount;
            });
        }

        graphStore.nodeLabels().forEach(label -> {
            var properties = nodeProperties.computeIfAbsent(label.name, k -> new HashMap<>());
            graphStore.schema().nodeSchema().propertySchemasFor(label).forEach(propertySchema -> {
                properties.put(propertySchema.key(), graphStore.nodeProperty(propertySchema.key()).values());
            });
        });

        return new NodeStore(
            graphStore.nodeCount(),
            labelCounts,
            nodeLabels,
            hasNodeLabels,
            nodeProperties.isEmpty() ? null : nodeProperties,
            additionalProperties
        );
    }
}
