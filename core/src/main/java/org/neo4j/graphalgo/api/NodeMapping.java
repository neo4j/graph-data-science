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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.NodeLabel;

import java.util.Collection;
import java.util.Set;

public interface NodeMapping extends IdMapping, NodeIterator, BatchNodeIterable {

    Set<NodeLabel> nodeLabels(long nodeId);

    void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer);

    Set<NodeLabel> availableNodeLabels();

    boolean hasLabel(long nodeId, NodeLabel label);

    default boolean containsOnlyAllNodesLabel() {
        return availableNodeLabels().size() == 1 && availableNodeLabels().contains(NodeLabel.ALL_NODES);
    }

    default NodeMapping withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        throw new UnsupportedOperationException("This node mapping does not support label filtering");
    }

    @FunctionalInterface
    interface NodeLabelConsumer {

        boolean accept(NodeLabel nodeLabel);

    }
}
