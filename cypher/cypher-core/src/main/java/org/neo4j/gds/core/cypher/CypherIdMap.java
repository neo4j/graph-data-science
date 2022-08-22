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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.IdMapAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CypherIdMap extends IdMapAdapter implements NodeLabelUpdater {

    private final Map<NodeLabel, BitSet> additionalNodeLabels;

    CypherIdMap(IdMap idMap) {
        super(idMap);
        this.additionalNodeLabels = new HashMap<>();
    }

    @Override
    public void addNodeLabel(NodeLabel nodeLabel) {
        this.additionalNodeLabels.put(nodeLabel, new BitSet(nodeCount()));
    }

    @Override
    public void addLabelToNode(long nodeId, NodeLabel nodeLabel) {
        additionalNodeLabels.computeIfAbsent(nodeLabel, ignore -> new BitSet(nodeCount()));
        additionalNodeLabels.get(nodeLabel).set(nodeId);
    }

    @Override
    public void removeLabelFromNode(long nodeId, NodeLabel nodeLabel) {
        var nodeLabelBitSet = additionalNodeLabels.get(nodeLabel);
        if (nodeLabelBitSet != null) {
            nodeLabelBitSet.clear(nodeId);
        } else {
            throw new RuntimeException(formatWithLocale("Could not find updatable label with name `%s`", nodeLabel.name()));
        }
    }

    @Override
    public List<NodeLabel> nodeLabels(long mappedNodeId) {
        var nodeLabels = new ArrayList<>(super.nodeLabels(mappedNodeId));
        additionalNodeLabels.forEach((nodeLabel, bitSet) -> {
            if (bitSet.get(mappedNodeId)) {
                nodeLabels.add(nodeLabel);
            }
        });
        return nodeLabels;
    }

    @Override
    public void forEachNodeLabel(long mappedNodeId, NodeLabelConsumer consumer) {
        super.forEachNodeLabel(mappedNodeId, consumer);
        for (Map.Entry<NodeLabel, BitSet> entry : additionalNodeLabels.entrySet()) {
            NodeLabel nodeLabel = entry.getKey();
            BitSet bitSet = entry.getValue();
            if (bitSet.get(mappedNodeId)) {
                if (!consumer.accept(nodeLabel)) {
                    break;
                }
            }
        }
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        var nodeLabels = new HashSet<>(super.availableNodeLabels());
        nodeLabels.addAll(additionalNodeLabels.keySet());
        return nodeLabels;
    }

    @Override
    public boolean hasLabel(long mappedNodeId, NodeLabel nodeLabel) {
        var hasLoadedLabel = super.hasLabel(mappedNodeId, nodeLabel);
        if (!hasLoadedLabel) {
            if (additionalNodeLabels.containsKey(nodeLabel)) {
                return additionalNodeLabels.get(nodeLabel).get(mappedNodeId);
            }
            return false;
        }
        return true;
    }
}
