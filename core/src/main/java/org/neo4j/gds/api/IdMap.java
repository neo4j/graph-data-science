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
import java.util.Optional;
import java.util.Set;

/**
 * Bidirectional mapping between two id spaces.
 * Usually the IdMap is used to map between neo4j
 * node ids and consecutive mapped node ids.
 */
public interface IdMap extends PartialIdMap, NodeIterator, BatchNodeIterable {

    /**
     * Defines the lower bound of mapped ids
     */
    long START_NODE_ID = 0;

    /**
     * Defines the value for unmapped ids
     */
    long NOT_FOUND = -1;

    /**
     * Map original nodeId to mapped nodeId
     *
     * Returns org.neo4j.gds.api.IdMap#NOT_FOUND if the nodeId is not mapped.
     */
    default long safeToMappedNodeId(long originalNodeId) {
        return highestOriginalId() < originalNodeId ? NOT_FOUND : toMappedNodeId(originalNodeId);
    };

    /**
     * Returns the original node id for the given mapped node id.
     * The original node id is typically the Neo4j node id.
     *
     * This method is guaranteed to always return the Neo4j id,
     * regardless of the given mapped node id refers to a filtered
     * node id space or a regular / unfiltered node id space.
     */
    long toOriginalNodeId(long mappedNodeId);

    /**
     * Maps a filtered mapped node id to its root mapped node id.
     * This is necessary for nested (filtered) id mappings.
     *
     * If this mapping is a nested mapping, this method
     * returns the root mapped node id of the parent mapping.
     * For the root mapping this method returns the given
     * node id.
     */
    long toRootNodeId(long mappedNodeId);

    /**
     * Returns true iff the Neo4j id is mapped, otherwise false.
     */
    boolean containsOriginalId(long originalNodeId);

    /**
     * Number of mapped nodeIds.
     */
    long nodeCount();

    /**
     * Number of mapped nodeIds for a specific node label.
     */
    long nodeCount(NodeLabel nodeLabel);

    /**
     * The highest id that is mapped in this id mapping.
     * <p>
     * The value is the upper bound of the original node id space.
     */
    long highestOriginalId();

    List<NodeLabel> nodeLabels(long mappedNodeId);

    void forEachNodeLabel(long mappedNodeId, IdMap.NodeLabelConsumer consumer);

    Set<NodeLabel> availableNodeLabels();

    boolean hasLabel(long mappedNodeId, NodeLabel label);

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
     * @param nodeId the node id to assign
     * @param nodeLabel the node label to which the node will be assigned to
     */
    void addNodeIdToLabel(long nodeId, NodeLabel nodeLabel);

    /**
     * Returns the original node mapping if the current node mapping is filtered, otherwise
     * it returns itself.
     */
    IdMap rootIdMap();

    default Optional<FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        throw new UnsupportedOperationException("This node mapping does not support label filtering");
    }

    @FunctionalInterface
    interface NodeLabelConsumer {

        boolean accept(NodeLabel nodeLabel);

    }
}
