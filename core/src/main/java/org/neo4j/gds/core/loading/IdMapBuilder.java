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

public interface IdMapBuilder {

    /**
     * Instantiate an allocator that accepts exactly {@code batchLength} many original ids.
     * <p>
     * Calling {@link org.neo4j.gds.core.loading.IdMapAllocator#insert(long[])} on the
     * returned allocator requires an array of length {@code batchLength}.
     * <p>
     * This method is thread-safe and intended to be called by multiple node importer threads.
     *
     * @return a non-thread-safe allocator for writing ids to the IdMap
     */
    IdMapAllocator allocate(int batchLength);

    IdMap build(
        LabelInformation.Builder labelInformationBuilder,
        long highestNodeId,
        int concurrency
    );
}
