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
package org.neo4j.gds.core.cypher;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.NodeMapping;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class CypherNodeMapping extends NodeMappingAdapter implements NodeLabelUpdater {

    private final Map<NodeLabel, BitSet> additionalNodeLabels;

    CypherNodeMapping(NodeMapping nodeMapping) {
        super(nodeMapping);
        this.additionalNodeLabels = new HashMap<>();
    }

    @Override
    public void addNodeLabel(NodeLabel nodeLabel) {
        if (!super.availableNodeLabels().contains(nodeLabel)) {
            this.additionalNodeLabels.put(nodeLabel, new BitSet(nodeCount()));
        } else {
            throw new IllegalArgumentException(formatWithLocale("Node label %s already exists", nodeLabel));
        }
    }

    @Override
    public void addLabelToNode(long nodeId, NodeLabel nodeLabel) {
        additionalNodeLabels.putIfAbsent(nodeLabel, new BitSet(nodeCount()));
        additionalNodeLabels.get(nodeLabel).set(nodeId);
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        var nodeLabels = new HashSet<>(super.nodeLabels(nodeId));
        additionalNodeLabels.forEach((nodeLabel, bitSet) -> {
            if (bitSet.get(nodeId)) {
                nodeLabels.add(nodeLabel);
            }
        });
        return nodeLabels;
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        super.forEachNodeLabel(nodeId, consumer);
        additionalNodeLabels.forEach((nodeLabel, bitSet) -> {
            if (bitSet.get(nodeId)) {
                consumer.accept(nodeLabel);
            }
        });
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        var nodeLabels = new HashSet<>(super.availableNodeLabels());
        nodeLabels.addAll(additionalNodeLabels.keySet());
        return nodeLabels;
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel nodeLabel) {
        var hasLoadedLabel = super.hasLabel(nodeId, nodeLabel);
        if (!hasLoadedLabel) {
            if (additionalNodeLabels.containsKey(nodeLabel)) {
                return additionalNodeLabels.get(nodeLabel).get(nodeId);
            }
            return false;
        }
        return true;
    }
}
