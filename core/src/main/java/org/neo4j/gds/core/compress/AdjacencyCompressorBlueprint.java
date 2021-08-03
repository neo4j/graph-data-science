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
package org.neo4j.gds.core.compress;

public interface AdjacencyCompressorBlueprint {

    /**
     * @return a copy of this blueprint that can be used concurrently with other copies to compress data.
     */
    AdjacencyCompressor createCompressor();

    /**
     * @return true iff this compressor can deal with properties.
     */
    boolean supportsProperties();

    /**
     * Implementors may choose to buffer some internal data and only write it intermittently to its final location.
     * This method is called at the end to ensure that all possible in-flight data can be cleaned up.
     *
     * This method might be called multiple times and any other method may be called after a flush.
     * To release internal data for good, see {@link AdjacencyCompressor#close()}.
     */
    void flush();

    /**
     * @return the final adjacency list, together with any number of properties, if any.
     */
    AdjacencyListsWithProperties build();
}
