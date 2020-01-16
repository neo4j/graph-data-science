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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;

public class NonWeightedPageRankVariant implements PageRankVariant {

    @Override
    public NonWeightedComputeStep createComputeStep(
            double dampingFactor,
            double toleranceValue,
            long[] sourceNodeIds,
            Graph graph,
            AllocationTracker tracker,
            int partitionSize,
            long start,
            DegreeCache aggregatedDegrees,
            long nodeCount
    ) {
        return new NonWeightedComputeStep(
                dampingFactor,
                toleranceValue,
                sourceNodeIds,
                graph,
                tracker,
                partitionSize,
                start
        );
    }

    @Override
    public DegreeComputer degreeComputer(Graph graph) {
        return new NoOpDegreeComputer();
    }

    class NoOpDegreeComputer implements DegreeComputer {
        @Override
        public DegreeCache degree(
                ExecutorService executor,
                int concurrency,
                AllocationTracker tracker) {
            return DegreeCache.EMPTY;
        }
    }
}
