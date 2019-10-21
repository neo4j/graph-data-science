/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

public interface NodeOrRelationshipProperties extends NodeProperties, RelationshipProperties {

    /**
     * Returns the proeprty value for the relationship defined by their start and end nodes.
     */
    double relationshipProperty(long sourceNodeId, long targetNodeId);

    @Override
    default double nodeProperty(long nodeId) {
        return relationshipProperty(nodeId, -1L);
    }

    /**
     * Returns the property value for a node or the given default value if no property had been defined.
     * The default value has precedence over the default value defined by the loader.
     */
    default double nodeProperty(long nodeId, double defaultValue) {
        return relationshipProperty(nodeId, -1L, defaultValue);
    }

    /**
     * Returns the maximum value contained in the mapping or {@code defaultValue} if the mapping is empty.
     *
     * @param defaultValue value being returned if the mapping is empty
     * @return maximum value or given default value if mapping is empty
     */
    default long getMaxPropertyValue(long defaultValue) {
        return getMaxPropertyValue().orElse(defaultValue);
    }

    /**
     * Release internal data structures and return an estimate how many bytes were freed.
     *
     * Note that the mapping is not usable afterwards.
     */
    long release();

    /**
     * Returns the number of values stored in that mapping.
     */
    long size();
}
