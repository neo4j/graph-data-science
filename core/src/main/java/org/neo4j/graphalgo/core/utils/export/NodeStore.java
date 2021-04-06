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
package org.neo4j.graphalgo.core.utils.export;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeIntArray;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeStore {

    static final String[] EMPTY_LABELS = new String[0];

    final long nodeCount;

    final HugeIntArray labelCounts;

    final NodeMapping nodeMapping;

    final Map<String, Map<String, NodeProperties>> nodeProperties;

    private final Set<NodeLabel> availableNodeLabels;

    private final boolean hasLabels;

    public NodeStore(
        long nodeCount,
        HugeIntArray labelCounts,
        NodeMapping nodeMapping,
        boolean hasLabels,
        Map<String, Map<String, NodeProperties>> nodeProperties
    ) {
        this.nodeCount = nodeCount;
        this.labelCounts = labelCounts;
        this.nodeMapping = nodeMapping;
        this.nodeProperties = nodeProperties;
        this.hasLabels = hasLabels;
        this.availableNodeLabels = nodeMapping.availableNodeLabels();
    }

    boolean hasLabels() {
        return hasLabels;
    }

    boolean hasProperties() {
        return nodeProperties != null;
    }

    int labelCount() {
        return !hasLabels() ? 0 : nodeMapping.availableNodeLabels().size();
    }

    public int propertyCount() {
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
            if (nodeMapping.hasLabel(nodeId, nodeLabel)) {
                labels[i++] = nodeLabel.name;
            }
        }

        return labels;
    }

    static NodeStore of(GraphStore graphStore, AllocationTracker tracker) {
        HugeIntArray labelCounts = null;
        Map<String, Map<String, NodeProperties>> nodeProperties;

        var nodeLabels = graphStore.nodes();

        boolean hasNodeLabels = !graphStore.schema().nodeSchema().containsOnlyAllNodesLabel();
        if (hasNodeLabels) {
            labelCounts = HugeIntArray.newArray(graphStore.nodeCount(), tracker);
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

        if (graphStore.nodePropertyKeys().isEmpty()) {
            nodeProperties = null;
        } else {
            nodeProperties = graphStore.nodePropertyKeys().entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().name,
                entry -> entry.getValue().stream().collect(Collectors.toMap(
                    propertyKey -> propertyKey,
                    propertyKey -> graphStore.nodePropertyValues(entry.getKey(), propertyKey)
                ))
            ));
        }

        return new NodeStore(
            graphStore.nodeCount(),
            labelCounts,
            nodeLabels,
            hasNodeLabels,
            nodeProperties
        );
    }
}
