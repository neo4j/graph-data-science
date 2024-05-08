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
package org.neo4j.gds.paths.yens;

import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.Estimate;
import org.neo4j.gds.paths.dijkstra.DijkstraMemoryEstimateDefinition;
import org.neo4j.gds.paths.dijkstra.DijkstraMemoryEstimateParameters;

public class YensMemoryEstimateDefinition implements MemoryEstimateDefinition {

    private final int numberOfShortestPathsToFind;

    public YensMemoryEstimateDefinition(int numberOfShortestPathsToFind) {
        this.numberOfShortestPathsToFind = numberOfShortestPathsToFind;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(Yens.class)
            .perThread("Yens Task", MemoryEstimations.builder(YensTask.class)
                .fixed("neighbors", Estimate.sizeOfLongArray(numberOfShortestPathsToFind))
                .add(
                    "Dijkstra",
                    new DijkstraMemoryEstimateDefinition(new DijkstraMemoryEstimateParameters(true, false))
                        .memoryEstimation()
                ).build())
            .build();
    }

}
