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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.api.IdMap;

/**
 * Extends the IdMap to support an additional
 * filtered id mapping layer.
 *
 * The mapping layers are called the following:
 * neo4jNodeId -> mappedNodeId -> filteredNodeId
 *
 * The first mapping layer (mappedNodeId) is also
 * referred to as rootNodeId.
 *
 * Note that functions like {@link #toOriginalNodeId(long)} or {@link #toMappedNodeId(long)}
 * will return the outermost or innermost mapped values respectively.
 */
public interface FilteredIdMap extends IdMap {

    /**
     * Maps a root mapped node id to a filtered mapped node id.
     * This is necessary for nested (filtered) id mappings.
     *
     * If this mapping is a nested mapping, this method
     * returns the mapped id corresponding to the mapped
     * id of the parent mapping.
     * For the root mapping this method returns the given
     * node id.
     */
    long rootToMappedNodeId(long rootNodeId);

    /**
     * Checks if the rootNodeId (mappedNodeId) is
     * present in the IdMaps mapping information.
     */
    boolean containsRootNodeId(long rootNodeId);
}
