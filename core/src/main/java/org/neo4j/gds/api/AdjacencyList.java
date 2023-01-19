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

import org.jetbrains.annotations.Nullable;

/**
 * The adjacency list for a mono-partite graph with an optional single relationship property.
 * Provides access to the {@link #degree(long) degree}, and the {@link #adjacencyCursor(long) target ids}.
 * The methods in here are not final and may be revised under the continuation of
 * Adjacency Compression III – Return of the Iterator
 * One particular change could be that properties will be returned from {@link AdjacencyCursor}s
 * instead from separate {@link PropertyCursor}s.
 */
public interface AdjacencyList {

    /**
     * Returns the degree of a node.
     *
     * Undefined behavior if the node does not exist.
     */
    int degree(long node);

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * The cursor is not expected to return correct property values.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties is unclear.
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
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties is unclear.
     *
     * Undefined behavior if the node does not exist.
     */
    AdjacencyCursor adjacencyCursor(long node, double fallbackValue);

    /**
     * Create a new cursor for the target ids of the given {@code node}.
     * The cursor is not expected to return correct property values.
     *
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties is unclear.
     *
     * The implementation might try to reuse the provided {@code reuse} cursor, if possible.
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
     * NOTE: Whether and how {@code AdjacencyCursor}s will return properties is unclear.
     *
     * The implementation might try to reuse the provided {@code reuse} cursor, if possible.
     * That is not guaranteed, however, implementation may choose to ignore the reuse cursor for any reason.
     *
     * Undefined behavior if the node does not exist.
     */
    default AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        return adjacencyCursor(node, fallbackValue);
    }

    /**
     * Create a new uninitialized cursor.
     *
     * NOTE: In order to use the returned cursor {@link AdjacencyCursor#init} must be called.
     */
    AdjacencyCursor rawAdjacencyCursor();

    AdjacencyList EMPTY = new AdjacencyList() {
        @Override
        public int degree(long node) {
            return 0;
        }

        @Override
        public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
            return AdjacencyCursor.empty();
        }

        @Override
        public AdjacencyCursor rawAdjacencyCursor() {
            return AdjacencyCursor.empty();
        }

    };
}
