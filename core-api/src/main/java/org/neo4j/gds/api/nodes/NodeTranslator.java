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

/*
 * Bidirectional mapping between two id spaces.
 */
public interface NodeTranslator {
    /**
     * Defines the value for unmapped ids
     */
    long NOT_FOUND = -1;

    /**
     * Number of mapped nodeIds.
     */
    long nodeCount();

    /**
     * The highest id that is mapped in this id mapping.
     * <p>
     * The value is the upper bound of the original node id space.
     */
    long highestOriginalId();

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
     * Maps an original node id to a mapped node id.
     * In case of nested id maps, the mapped node id
     * is always in the space of the innermost mapping.
     *
     * @param originalNodeId must be smaller or equal to the id returned by {@link org.neo4j.gds.api.nodes.IdMap#highestOriginalId}
     */
    long toMappedNodeId(long originalNodeId);

    /**
     * Returns true iff the Neo4j id is mapped, otherwise false.
     */
    boolean containsOriginalId(long originalNodeId);

    /**
     * A unique identifier for this type of IdMap.
     */
    String typeId();

    /**
     * Map original nodeId to mapped nodeId
     *
     * Returns org.neo4j.gds.api.nodes.IdMap#NOT_FOUND if the nodeId is not mapped.
     */
    default long safeToMappedNodeId(long originalNodeId) {
        return highestOriginalId() < originalNodeId ? NOT_FOUND : toMappedNodeId(originalNodeId);
    }
}
