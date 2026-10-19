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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.memest.GraphStoreEstimationService;

public class UnprojectedAlgorithmEstimationService {
    private final GraphStoreEstimationService graphStoreEstimationService;

    public UnprojectedAlgorithmEstimationService(
        GraphStoreEstimationService graphStoreEstimationService
    ) {
        this.graphStoreEstimationService = graphStoreEstimationService;
    }

    public MemoryEstimateResult estimate(
        RequestCorrelationId requestCorrelationId,
        GraphProjectConfig graphProjectConfig,
        MemoryEstimation memoryEstimation,
        Concurrency concurrency,
        DimensionTransformer dimensionTransformer
    ) {
        var estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        var graphMemoryEstimation = graphStoreEstimationService.estimate(
            requestCorrelationId,
            graphProjectConfig
        );

        estimationBuilder.add("graph", graphMemoryEstimation.estimateMemoryUsageAfterLoading());

        var graphDimensions = graphMemoryEstimation.dimensions();
        var transformedDimensions = dimensionTransformer.transform(graphDimensions);

        return AlgorithmEstimator.estimate(
            estimationBuilder,
            memoryEstimation,
            transformedDimensions,
            concurrency
        );
    }
}
