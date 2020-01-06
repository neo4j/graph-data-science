/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.mem;

import org.neo4j.graphalgo.core.GraphDimensions;

public interface Assessable {

    /**
     * Returns an estimation about the memory consumption of that algorithm. The memory estimation can be used to
     * compute the actual consumption depending on {@link GraphDimensions} and concurrency.
     *
     * @return memory estimation
     * @see MemoryEstimations
     * @see MemoryEstimation#estimate(GraphDimensions, int)
     */
    MemoryEstimation memoryEstimation();

    /**
     * Computes the memory consumption for the algorithm depending on the given {@link GraphDimensions} and concurrency.
     *
     * This is shorthand for {@link MemoryEstimation#estimate(GraphDimensions, int)}.
     *
     * @param dimensions  graph dimensions
     * @param concurrency concurrency which is used to run the algorithm
     * @return memory requirements
     */
    default MemoryTree memoryEstimation(GraphDimensions dimensions, int concurrency) {
        return memoryEstimation().estimate(dimensions, concurrency);
    }

}
