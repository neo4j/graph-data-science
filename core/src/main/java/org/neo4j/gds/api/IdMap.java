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
package org.neo4j.gds.api;

import org.neo4j.gds.NodeLabel;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Bi-directional mapping between two id spaces.
 */
public interface IdMap extends PartialIdMap, NodeIterator, BatchNodeIterable {

    /**
     * Defines the lower bound of mapped ids
     * TODO: function?
     */
    long START_NODE_ID = 0;

    /**
     * Defines the value for unmapped ids
     */
    long NOT_FOUND = -1;

    /**
     * Map original nodeId to inner nodeId
     *
     * Returns org.neo4j.gds.api.IdMap#NOT_FOUND if the nodeId is not mapped.
     */
    default long safeToMappedNodeId(long nodeId) {
        return highestNeoId() < nodeId ? NOT_FOUND : toMappedNodeId(nodeId);
    };

    /**
     * Map inner nodeId back to original nodeId
     */
    long toOriginalNodeId(long nodeId);

    /**
     * Maps an internal id to its root internal node id.
     * This is necessary for nested (filtered) id mappings.
     *
     * If this mapping is a nested mapping, this method
     * returns the root node id of the parent mapping.
     * For the root mapping this method returns the given
     * node id.
     */
    long toRootNodeId(long nodeId);

    /**
     * Maps a root internal node id to an internal node id.
     * This is necessary for nested (filtered) id mappings.
     *
     * If this mapping is a nested mapping, this method
     * returns the mapped id corresponding to the mapped
     * id of the parent mapping.
     * For the root mapping this method returns the given
     * node id.
     */
    long fromRootNodeId(long rootNodeId);

    /**
     * Returns true iff the nodeId is mapped, otherwise false.
     */
    boolean contains(long nodeId);

    /**
     * Number of mapped nodeIds.
     */
    long nodeCount();

    long highestNeoId();

    List<NodeLabel> nodeLabels(long nodeId);

    void forEachNodeLabel(long nodeId, IdMap.NodeLabelConsumer consumer);

    Set<NodeLabel> availableNodeLabels();

    boolean hasLabel(long nodeId, NodeLabel label);

    /**
     * Returns the original node mapping if the current node mapping is filtered, otherwise
     * it returns itself.
     */
    IdMap rootIdMap();

    default IdMap withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        throw new UnsupportedOperationException("This node mapping does not support label filtering");
    }

    @FunctionalInterface
    interface NodeLabelConsumer {

        boolean accept(NodeLabel nodeLabel);

    }
}
