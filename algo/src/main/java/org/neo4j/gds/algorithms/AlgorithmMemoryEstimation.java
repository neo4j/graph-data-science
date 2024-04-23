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
package org.neo4j.gds.algorithms;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;

/*
A copy of `org.neo4j.gds.executor.ProcedureMemoryEstimation` so we don't introduce circular dependency to `:executor`...
 */
public class AlgorithmMemoryEstimation<ALGO extends Algorithm<?>, CONFIG extends AlgoBaseConfig> {
    private final AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory;
    private final GraphDimensions graphDimensions;

    public AlgorithmMemoryEstimation(
        GraphDimensions graphDimensions,
        AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory
    ) {
        this.graphDimensions = graphDimensions;
        this.algorithmFactory = algorithmFactory;
    }

    public MemoryTreeWithDimensions memoryEstimation(CONFIG config) {
        var estimationBuilder = MemoryEstimations.builder("Algorithm Memory Estimation");

        estimationBuilder.add("Algorithm", algorithmFactory.memoryEstimation(config));

        // this is a bit tricky, only used in ML Pipelines case but I am a bit reluctant to remove it yet...
        var extendedDimension = algorithmFactory.estimatedGraphDimensionTransformer(graphDimensions, config);

        var memoryTree = estimationBuilder.build().estimate(extendedDimension, config.typedConcurrency());
        return new MemoryTreeWithDimensions(memoryTree, graphDimensions);
    }
}
