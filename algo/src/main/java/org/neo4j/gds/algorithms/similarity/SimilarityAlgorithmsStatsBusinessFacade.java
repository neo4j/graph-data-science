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
package org.neo4j.gds.algorithms.similarity;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFields;
import org.neo4j.gds.algorithms.similarity.specificfields.SimilaritySpecificFieldsWithDistribution;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.result.SimilarityStatistics;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStatsConfig;

import java.util.function.Function;
import java.util.function.Supplier;

import static org.neo4j.gds.algorithms.similarity.SimilarityResultCompanion.NODE_SIMILARITY_SPECIFIC_FIELDS_SUPPLIER;

public class SimilarityAlgorithmsStatsBusinessFacade {

    private final SimilarityAlgorithmsFacade similarityAlgorithmsFacade;

    public SimilarityAlgorithmsStatsBusinessFacade(SimilarityAlgorithmsFacade similarityAlgorithmsFacade) {
        this.similarityAlgorithmsFacade = similarityAlgorithmsFacade;
    }

    public StatsResult<SimilaritySpecificFieldsWithDistribution> filteredNodeSimilarity(
        String graphName,
        FilteredNodeSimilarityStatsConfig configuration,
        boolean computeSimilarityDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> similarityAlgorithmsFacade.filteredNodeSimilarity(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return statsResult(
            algorithmResult,
            result -> result.graphResult(),
            NODE_SIMILARITY_SPECIFIC_FIELDS_SUPPLIER,
            intermediateResult.computeMilliseconds,
            () -> SimilaritySpecificFieldsWithDistribution.EMPTY,
            computeSimilarityDistribution
        );
    }

    <RESULT, ASF extends SimilaritySpecificFields> StatsResult<ASF> statsResult(
        AlgorithmComputationResult<RESULT> algorithmResult,
        Function<RESULT, SimilarityGraphResult> similarityGraphResultSupplier,
        SpecificFieldsWithSimilarityStatisticsSupplier<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        boolean shouldComputeSimilarityDistribution
    ) {

        return algorithmResult.result().map(result -> {


            // 2. Compute result statistics
            var communityStatistics = SimilarityStatistics.similarityStats(
                () -> similarityGraphResultSupplier.apply(result).similarityGraph(),
                shouldComputeSimilarityDistribution
            );

            var similaritySummary = SimilarityStatistics.similaritySummary(communityStatistics.histogram());

            //3. Create the specific fields
            var specificFields = specificFieldsSupplier.specificFields(
                result,
                similaritySummary
            );

            //4. Produce the results
            return StatsResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(communityStatistics.computeMilliseconds())
                .algorithmSpecificFields(specificFields)
                .build();

        }).orElseGet(() -> StatsResult.empty(emptyASFSupplier.get()));

    }

}
