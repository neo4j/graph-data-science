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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.collection.primitive.PrimitiveLongIterable;

public interface HugeParallelGraphImporter<T extends Runnable> {

    /**
     * Return a new {@link Runnable}.
     * This method is called on each Thread that performs the importing,
     * possibly in concurrently, so this method must be thread-safe.
     * Depending on the batch size and the number of available threads,
     * the same thread may call this method multiple times.
     * If this methods returns same instances across multiple calls from multiple
     * threads, those instances have to be thread-safe as well.
     */
    T newImporter(long nodeOffset, PrimitiveLongIterable nodeIds);
}
