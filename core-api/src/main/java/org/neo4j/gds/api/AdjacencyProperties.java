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
 * The properties for a mono-partite graph for a single relationship property.
 * Provides access to the target {@link #propertyCursor(long) properties} for any given source node.
 */
public interface AdjacencyProperties {

    /**
     * Create a new cursor for the properties of the relationships of a given {@code node}.
     * The cursor is expected to produce property values.
     *
     * Undefined behavior if the node does not exist.
     * Undefined behavior if this list does not have properties.
     */
    default PropertyCursor propertyCursor(long node) {
        return propertyCursor(node, Double.NaN);
    }

    /**
     * Create a new cursor for the properties of the relationships of a given {@code node}.
     * If the cursor cannot produce property values, it will yield the provided {@code fallbackValue}.
     *
     * NOTE: Fallback behavior is not widely available and will be part of the next episode.
     *
     * Undefined behavior if the node does not exist.
     */
    PropertyCursor propertyCursor(long node, double fallbackValue);

    /**
     * Create a new cursor for the properties of the relationships of a given {@code node}.
     * The cursor is expected to produce property values.
     *
     * The Implementation might try to reuse the provided {@code reuse} cursor, if possible.
     * That is not guaranteed, however, implementation may choose to ignore the reuse cursor for any reason.
     *
     * Undefined behavior if the node does not exist.
     * Undefined behavior if this list does not have properties.
     */
    default PropertyCursor propertyCursor(PropertyCursor reuse, long node) {
        return propertyCursor(reuse, node, Double.NaN);
    }

    /**
     * Create a new cursor for the properties of the relationships of a given {@code node}.
     * If the cursor cannot produce property values, it will yield the provided {@code fallbackValue}.
     *
     * NOTE: Fallback behavior is not widely available and will be part of the next episode.
     *
     * The Implementation might try to reuse the provided {@code reuse} cursor, if possible.
     * That is not guaranteed, however, implementation may choose to ignore the reuse cursor for any reason.
     *
     * Undefined behavior if the node does not exist.
     */
    default PropertyCursor propertyCursor(PropertyCursor reuse, long node, double fallbackValue) {
        return propertyCursor(node, fallbackValue);
    }

    /**
     * Create a new uninitialized cursor.
     *
     * NOTE: In order to use the returned cursor {@link PropertyCursor#init} must be called.
     */
    PropertyCursor rawPropertyCursor();
}
