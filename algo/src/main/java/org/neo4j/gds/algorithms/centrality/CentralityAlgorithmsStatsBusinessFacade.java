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
package org.neo4j.gds.algorithms.centrality;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.centrality.specificfields.CentralityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.centrality.specificfields.DefaultCentralitySpecificFields;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.betweenness.BetweennessCentralityStatsConfig;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.degree.DegreeCentralityStatsConfig;
import org.neo4j.gds.result.CentralityStatistics;

import java.util.function.Supplier;

public class CentralityAlgorithmsStatsBusinessFacade {

    private final CentralityAlgorithmsFacade centralityAlgorithmsFacade;

    public CentralityAlgorithmsStatsBusinessFacade(CentralityAlgorithmsFacade centralityAlgorithmsFacade) {
        this.centralityAlgorithmsFacade = centralityAlgorithmsFacade;
    }


    public StatsResult<DefaultCentralitySpecificFields> betweennessCentrality(
        String graphName,
        BetweennessCentralityStatsConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> centralityAlgorithmsFacade.betweennessCentrality(graphName, configuration)
        );

        return statsResult(
            intermediateResult.algorithmResult,
            configuration,
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds
        );
    }

    public StatsResult<DefaultCentralitySpecificFields> degreeCentrality(
        String graphName,
        DegreeCentralityStatsConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean shouldComputeCentralityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> centralityAlgorithmsFacade.degreeCentrality(graphName, configuration, user, databaseId)
        );

        return statsResult(
            intermediateResult.algorithmResult,
            configuration,
            shouldComputeCentralityDistribution,
            intermediateResult.computeMilliseconds
        );
    }


    <RESULT extends CentralityAlgorithmResult, CONFIG extends AlgoBaseConfig> StatsResult<DefaultCentralitySpecificFields> statsResult(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        boolean shouldComputeCentralityDistribution,
        long computeMilliseconds
    ) {
        CentralityFunctionSupplier<RESULT> centralityFunctionSupplier = (r) -> r.centralityScoreProvider();
        SpecificFieldsWithCentralityDistributionSupplier<RESULT, DefaultCentralitySpecificFields> specificFieldsSupplier = (r, c) -> new DefaultCentralitySpecificFields(
            c);
        Supplier<DefaultCentralitySpecificFields> emptyASFSupplier = () -> DefaultCentralitySpecificFields.EMPTY;

        return statsResult(
            algorithmResult,
            configuration,
            centralityFunctionSupplier,
            specificFieldsSupplier,
            shouldComputeCentralityDistribution,
            computeMilliseconds,
            emptyASFSupplier
        );
    }

    <RESULT, CONFIG extends AlgoBaseConfig, ASF extends CentralityStatisticsSpecificFields> StatsResult<ASF> statsResult(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        CentralityFunctionSupplier<RESULT> centralityFunctionSupplier,
        SpecificFieldsWithCentralityDistributionSupplier<RESULT, ASF> specificFieldsSupplier,
        boolean shouldComputeCentralityDistribution,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier
    ) {

        return algorithmResult.result().map(result -> {

            // 2. Compute result statistics
            var centralityStatistics = CentralityStatistics.centralityStatistics(
                algorithmResult.graph().nodeCount(),
                centralityFunctionSupplier.centralityFunction(result),
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                shouldComputeCentralityDistribution
            );

            var communitySummary = CentralityStatistics.centralitySummary(centralityStatistics.histogram());

            var specificFields = specificFieldsSupplier.specificFields(result, communitySummary);

            return StatsResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(centralityStatistics.computeMilliseconds())
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> StatsResult.empty(emptyASFSupplier.get()));

    }
}
