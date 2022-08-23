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
import org.neo4j.gds.core.loading.FilteredIdMap;

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
        return highestNeoId() < originalNodeId ? NOT_FOUND : toMappedNodeId(originalNodeId);
    };

    /**
     * Map mapped nodeId back to neo4j nodeId
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
     * Returns true iff the neo4jNodeId is mapped, otherwise false.
     */
    boolean contains(long originalNodeId);

    /**
     * Number of mapped nodeIds.
     */
    long nodeCount();

    long highestNeoId();

    List<NodeLabel> nodeLabels(long mappedNodeId);

    void forEachNodeLabel(long mappedNodeId, IdMap.NodeLabelConsumer consumer);

    Set<NodeLabel> availableNodeLabels();

    boolean hasLabel(long mappedNodeId, NodeLabel label);

    /**
     * Returns the original node mapping if the current node mapping is filtered, otherwise
     * it returns itself.
     */
    IdMap rootIdMap();

    default Optional<? extends FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        throw new UnsupportedOperationException("This node mapping does not support label filtering");
    }

    @FunctionalInterface
    interface NodeLabelConsumer {

        boolean accept(NodeLabel nodeLabel);

    }
}
