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

/**
 * This interface exposes the relevant parts of {@link org.neo4j.gds.api.nodes.IdMap} used
 * for relationship loading. It helps implementations that are only used for relationship
 * loading to avoid implementing unnecessary methods. It is also used for deferred node
 * building completion, to start loading relationships before node loading is finalized.
 * For this reason, the node count from a PartialIdMap may not be reliable for all use
 * cases. When doing node and relationship loading at the same time, caller should check
 * whether the PartialIdMap is finalized and interpret the node count accordingly.
 */
public interface PartialIdMap {

    /**
     * Maps an original node id to a mapped node id.
     * In case of nested id maps, the mapped node id
     * is always in the space of the innermost mapping.
     *
     * @param originalNodeId must be smaller or equal to the id returned by {@link org.neo4j.gds.api.nodes.IdMap#highestOriginalId}
     */
    long toMappedNodeId(long originalNodeId);

    /**
     * Number of mapped node ids in the root mapping.
     * This is necessary for nested (filtered) id mappings.
     * May return 0 if the PartialIdMap has not yet been finalized.
     * If this matters, use {@code isFinalized()} to check.
     */
    long rootNodeCount();

    /**
     * Whether the PartialIdMap has been finalized such that the {@code rootNodeCount()} is reliable.
     * Most implementations are finalized, only the LazyIdMapBuilder implementation may be not finalized.
     */
    default boolean isFinalized() {
        return true;
    }
}
