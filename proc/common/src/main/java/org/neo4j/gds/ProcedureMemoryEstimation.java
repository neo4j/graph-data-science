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
package org.neo4j.gds;

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;

public class ProcedureMemoryEstimation<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig
> {

    private final GraphStoreLoader graphStoreLoader;
    private final AlgorithmFactory<ALGO, CONFIG> algorithmFactory;

    ProcedureMemoryEstimation(
        GraphStoreLoader graphStoreLoader,
        AlgorithmFactory<ALGO, CONFIG> algorithmFactory
    ) {
        this.graphStoreLoader = graphStoreLoader;
        this.algorithmFactory = algorithmFactory;
    }

    MemoryTreeWithDimensions memoryEstimation(CONFIG config) {
        MemoryEstimations.Builder estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        GraphDimensions estimateDimensions = graphStoreLoader.graphDimensions();
        graphStoreLoader.memoryEstimation().map(graphEstimation -> estimationBuilder.add("graph", graphEstimation));

        estimationBuilder.add("algorithm", algorithmFactory.memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(estimateDimensions, config.concurrency());
        return new MemoryTreeWithDimensions(memoryTree, estimateDimensions);
    }
}
