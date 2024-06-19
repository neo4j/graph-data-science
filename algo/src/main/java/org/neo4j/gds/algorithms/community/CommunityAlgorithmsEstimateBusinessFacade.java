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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationBaseConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMemoryEstimateDefinition;
import org.neo4j.gds.scc.SccBaseConfig;
import org.neo4j.gds.scc.SccMemoryEstimateDefinition;
import org.neo4j.gds.triangle.IntersectingTriangleCountMemoryEstimateDefinition;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMemoryEstimateDefinition;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;

import java.util.Optional;

public class CommunityAlgorithmsEstimateBusinessFacade {

    private final AlgorithmEstimator algorithmEstimator;

    public CommunityAlgorithmsEstimateBusinessFacade(
        AlgorithmEstimator algorithmEstimator
    ) {
        this.algorithmEstimator = algorithmEstimator;
    }

    public <C extends TriangleCountBaseConfig> MemoryEstimateResult triangleCount(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new IntersectingTriangleCountMemoryEstimateDefinition()
        );
    }

    public <C extends SccBaseConfig> MemoryEstimateResult estimateScc(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new SccMemoryEstimateDefinition()
        );
    }

    public <C extends LocalClusteringCoefficientBaseConfig> MemoryEstimateResult localClusteringCoefficient(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new LocalClusteringCoefficientMemoryEstimateDefinition(configuration.seedProperty())
        );
    }

    public <C extends ModularityOptimizationBaseConfig> MemoryEstimateResult modularityOptimization(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new ModularityOptimizationMemoryEstimateDefinition()
        );
    }
}
