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

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationBaseConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationFactory;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.scc.SccAlgorithmFactory;
import org.neo4j.gds.scc.SccCommonBaseConfig;
import org.neo4j.gds.triangle.IntersectingTriangleCountFactory;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientFactory;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;

import java.util.Optional;

public class CommunityAlgorithmsFacade {

    private final AlgorithmRunner algorithmRunner;

    public CommunityAlgorithmsFacade(
        AlgorithmRunner algorithmRunner
    ) {
        this.algorithmRunner = algorithmRunner;
    }

    AlgorithmComputationResult<TriangleCountResult> triangleCount(
        String graphName,
        TriangleCountBaseConfig config
    ) {
        return algorithmRunner.run(
            graphName,
            config,
            Optional.empty(),
            new IntersectingTriangleCountFactory<>()
        );
    }

    AlgorithmComputationResult<HugeLongArray> scc(
        String graphName,
        SccCommonBaseConfig config
    ) {
        return algorithmRunner.run(
            graphName,
            config,
            Optional.empty(),
            new SccAlgorithmFactory<>()
        );
    }

    public AlgorithmComputationResult<LocalClusteringCoefficientResult> localClusteringCoefficient(
        String graphName,
        LocalClusteringCoefficientBaseConfig config
    ) {
        return algorithmRunner.run(
            graphName,
            config,
            Optional.empty(),
            new LocalClusteringCoefficientFactory<>()
        );
    }

    public AlgorithmComputationResult<ModularityOptimizationResult> modularityOptimization(
        String graphName,
        ModularityOptimizationBaseConfig config
    ) {
        return algorithmRunner.run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new ModularityOptimizationFactory<>()
        );
    }
}
