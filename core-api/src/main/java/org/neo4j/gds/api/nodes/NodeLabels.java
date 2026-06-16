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
package org.neo4j.gds.api.nodes;

import org.neo4j.gds.NodeLabel;

import java.util.List;
import java.util.Set;

public interface NodeLabels {

    LabelInformation labelInformation();

    default Set<NodeLabel> availableNodeLabels() {
        return labelInformation().availableNodeLabels();
    }

    default List<NodeLabel> nodeLabels(long mappedNodeId) {
        return labelInformation().nodeLabelsForNodeId(mappedNodeId);
    }

    default boolean hasLabel(long mappedNodeId, NodeLabel nodeLabel) {
        return labelInformation().hasLabel(mappedNodeId, nodeLabel);
    }

    /**
     * Number of mapped nodeIds for a specific node label.
     */
    default long nodeCount(NodeLabel nodeLabel) {
        return labelInformation().nodeCountForLabel(nodeLabel);
    }

    default void forEachNodeLabel(long mappedNodeId, NodeLabelConsumer consumer) {
        labelInformation().forEachNodeLabel(mappedNodeId, consumer);
    }

    /**
     * Adds new node label to the available node labels.
     * The labels is not assigned to any nodes at this point.
     *
     * @param nodeLabel the node label to add
     */
    void addNodeLabel(NodeLabel nodeLabel);

    /**
     * Assigns a node to the given node label.
     *
     * @param mappedNodeId the node id to assign
     * @param nodeLabel the node label to which the node will be assigned to
     */
    void addNodeIdToLabel(long mappedNodeId, NodeLabel nodeLabel);
}
