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
package org.neo4j.gds.procedures.algorithms.similarity;

import org.neo4j.gds.algorithms.similarity.SimilarityResultCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.result.SimilarityStatistics;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Map;
import java.util.stream.Stream;

class SimilarityStatsProcessor {
    Stream<KnnStatsResult> process(
        Graph graph, ConcurrencyConfig concurrencyConfiguration,
        Stream<SimilarityResult> similarityResultStream,
        boolean shouldComputeSimilarityDistribution,
        AlgorithmProcessingTimings timings,
        Map<String, Object> configurationMap,
        long nodesCompared,
        long nodePairs,
        boolean didConverge,
        long ranIterations,
        long nodePairsConsidered
    ) {
        var similarityGraphResult = SimilarityResultCompanion.computeToGraph(
            graph,
            graph.nodeCount(),
            concurrencyConfiguration.typedConcurrency(),
            similarityResultStream
        );

        var communityStatistics = SimilarityStatistics.similarityStats(
            similarityGraphResult::similarityGraph,
            shouldComputeSimilarityDistribution
        );

        var similaritySummary = SimilarityStatistics.similaritySummary(communityStatistics.histogram());

        return Stream.of(
            new KnnStatsResult(
                timings.preProcessingMillis,
                timings.computeMillis,
                communityStatistics.computeMilliseconds(),
                nodesCompared,
                nodePairs,
                similaritySummary,
                didConverge,
                ranIterations,
                nodePairsConsidered,
                configurationMap
            )
        );
    }
}
