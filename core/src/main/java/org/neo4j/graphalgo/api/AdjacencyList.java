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

import org.jetbrains.annotations.Nullable;

/**
 * The adjacency list for a mono-partite graph with an optional single relationship property.
 * Provides access to the {@link #degree(long) degree}, the {@link #adjacencyCursor(long) target ids}, and {@link #propertyCursor(long) properties} for any given source node.
 * The methods in here are not final and may be revised under the continuation of
 * Adjacency Compression III – Return of the Iterator
 * One particular change could be that properties will be returned from {@link org.neo4j.graphalgo.api.AdjacencyCursor}s
 * instead from separate {@link org.neo4j.graphalgo.api.PropertyCursor}s.
 */
public interface AdjacencyList extends AdjacencyDegrees, AutoCloseable {

    /**
     * Returns the degree of a node.
     *
     * Undefined behavior if the node does not exist.
     */
    @Override
    int degree(long node);

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * The cursor is not expected to return correct property values.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties unclear.
     *
     * Undefined behavior if the node does not exist.
     */
    default AdjacencyCursor adjacencyCursor(long node) {
        return adjacencyCursor(node, Double.NaN);
    }

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * If the cursor cannot produce property values, it will yield the provided {@code fallbackValue}.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties unclear.
     *
     * Undefined behavior if the node does not exist.
     */
    AdjacencyCursor adjacencyCursor(long node, double fallbackValue);

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * The cursor is not expected to return correct property values.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties unclear.
     *
     * The Implementation might try to reuse the provided {@code reuse} cursor, if possible.
     * That is not guaranteed, however, implementation may choose to ignore the reuse cursor for any reason.
     *
     * Undefined behavior if the node does not exist.
     */
    default AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node) {
        return adjacencyCursor(reuse, node, Double.NaN);
    }

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * If the cursor cannot produce property values, it will yield the provided {@code fallbackValue}.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties unclear.
     *
     * The Implementation might try to reuse the provided {@code reuse} cursor, if possible.
     * That is not guaranteed, however, implementation may choose to ignore the reuse cursor for any reason.
     *
     * Undefined behavior if the node does not exist.
     */
    default AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        return adjacencyCursor(node, fallbackValue);
    }

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
     *  The cursor is expected to produce property values.
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

    @Override
    void close();
}
