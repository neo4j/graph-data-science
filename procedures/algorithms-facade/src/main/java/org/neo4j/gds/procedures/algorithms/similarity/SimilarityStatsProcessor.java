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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityResultStreamDelegate;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.result.SimilarityStatistics;
import org.neo4j.gds.similarity.SimilarityGraph;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.stream.Stream;

class SimilarityStatsProcessor {
    private final SimilarityResultStreamDelegate similarityResultStreamDelegate = new SimilarityResultStreamDelegate();

    static SimilarityStatistics.SimilarityDistributionResults EMPTY = new SimilarityStatistics.SimilarityDistributionResults(
        Map.of(),
        0
    );

    SimilarityStatistics.SimilarityDistributionResults computeSimilarityDistribution(
        Graph graph,
        ConcurrencyConfig concurrencyConfiguration,
        Stream<SimilarityResult> similarityResultStream,
        boolean shouldComputeSimilarityDistribution,
        TerminationFlag terminationFlag
    ) {
        if (!shouldComputeSimilarityDistribution) return EMPTY;
        var tStart = System.currentTimeMillis();
        var similarityGraphResult = similarityResultStreamDelegate.computeSimilarityGraph(
            graph,
            concurrencyConfiguration.concurrency(),
            similarityResultStream,
            shouldComputeSimilarityDistribution,
            terminationFlag
        );
        var tEnd = System.currentTimeMillis();
        var result = computeSimilarityDistribution(shouldComputeSimilarityDistribution, similarityGraphResult);
        return new SimilarityStatistics.SimilarityDistributionResults(
            result.distribution(),
            result.computeMilliseconds() + (tEnd - tStart)
        );
    }

    SimilarityStatistics.SimilarityDistributionResults computeSimilarityDistribution(
        boolean shouldComputeSimilarityDistribution,
        SimilarityGraph similarityGraphResult
    ) {
        if (!shouldComputeSimilarityDistribution) return EMPTY;
        var tStart = System.currentTimeMillis();
        var result = similarityGraphResult.similarityDistribution();
        var tEnd = System.currentTimeMillis();
        return new SimilarityStatistics.SimilarityDistributionResults(result, tEnd - tStart);
    }


}
