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
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.community.specificfields.CommunityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.LocalClusteringCoefficientSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.ModularityOptimizationSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.StandardCommunityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.TriangleCountSpecificFields;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStatsConfig;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.scc.SccStatsConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStatsConfig;
import org.neo4j.gds.triangle.TriangleCountStatsConfig;

import java.util.function.Supplier;

import static org.neo4j.gds.algorithms.runner.AlgorithmRunner.runWithTiming;

public class CommunityAlgorithmsStatsBusinessFacade {
    private final CommunityAlgorithmsFacade communityAlgorithmsFacade;

    public CommunityAlgorithmsStatsBusinessFacade(CommunityAlgorithmsFacade communityAlgorithmsFacade) {
        this.communityAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public StatsResult<LocalClusteringCoefficientSpecificFields> localClusteringCoefficient(
        String graphName,
        LocalClusteringCoefficientStatsConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.localClusteringCoefficient(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return statsResult(
            algorithmResult,
            (result) -> new LocalClusteringCoefficientSpecificFields(
                result.localClusteringCoefficients().size(),
                result.averageClusteringCoefficient()
            ),
            intermediateResult.computeMilliseconds,
            () -> LocalClusteringCoefficientSpecificFields.EMPTY
        );
    }

    public StatsResult<StandardCommunityStatisticsSpecificFields> scc(
        String graphName,
        SccStatsConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.scc(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return statsResult(
            algorithmResult,
            configuration,
            (result -> result::get),
            (result, componentCount, communitySummary) -> {
                return new StandardCommunityStatisticsSpecificFields(
                    componentCount,
                    communitySummary
                );
            },
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> StandardCommunityStatisticsSpecificFields.EMPTY
        );
    }

    public StatsResult<TriangleCountSpecificFields> triangleCount(
        String graphName,
        TriangleCountStatsConfig config
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.triangleCount(graphName, config)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return statsResult(
            algorithmResult,
            (result) -> new TriangleCountSpecificFields(result.globalTriangles(), algorithmResult.graph().nodeCount()),
            intermediateResult.computeMilliseconds,
            () -> TriangleCountSpecificFields.EMPTY
        );
    }

    public StatsResult<ModularityOptimizationSpecificFields> modularityOptimization(
        String graphName,
        ModularityOptimizationStatsConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.modularityOptimization(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return statsResult(
            algorithmResult,
            configuration,
            (result -> result::communityId),
            (result, componentCount, communitySummary) -> {
                return new ModularityOptimizationSpecificFields(
                    result.modularity(),
                    result.ranIterations(),
                    result.didConverge(),
                    result.asNodeProperties().nodeCount(),
                    componentCount,
                    communitySummary
                );
            },
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> ModularityOptimizationSpecificFields.EMPTY
        );
    }


    /*
    By using `ASF extends CommunityStatisticsSpecificFields` we enforce the algorithm specific fields
    to contain the statistics information.
    */
    <RESULT, CONFIG extends AlgoBaseConfig, ASF extends CommunityStatisticsSpecificFields> StatsResult<ASF> statsResult(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        CommunityFunctionSupplier<RESULT> communityFunctionSupplier,
        SpecificFieldsWithCommunityStatisticsSupplier<RESULT, ASF> specificFieldsSupplier,
        StatisticsComputationInstructions statisticsComputationInstructions,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier
    ) {

        return algorithmResult.result().map(result -> {

            // 2. Compute result statistics
            var communityStatistics = CommunityStatistics.communityStats(
                algorithmResult.graph().nodeCount(),
                communityFunctionSupplier.communityFunction(result),
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                statisticsComputationInstructions
            );

            var componentCount = communityStatistics.componentCount();
            var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram());

            var specificFields = specificFieldsSupplier.specificFields(result, componentCount, communitySummary);

            return StatsResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(communityStatistics.computeMilliseconds())
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> StatsResult.empty(emptyASFSupplier.get()));

    }

    <RESULT, CONFIG extends AlgoBaseConfig, ASF> StatsResult<ASF> statsResult(
        AlgorithmComputationResult<RESULT> algorithmResult,
        SpecificFieldsSupplier<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier
    ) {

        return algorithmResult.result().map(result -> {

            var specificFields = specificFieldsSupplier.specificFields(result);

            return StatsResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(0)
                .algorithmSpecificFields(specificFields)
                .build();

        }).orElseGet(() -> StatsResult.empty(emptyASFSupplier.get()));

    }
}
