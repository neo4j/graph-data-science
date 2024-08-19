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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityResultStreamDelegate;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStatsConfig;

import java.util.Optional;
import java.util.stream.Stream;

class FilteredKnnResultBuilderForStatsMode implements StatsResultBuilder<FilteredKnnStatsConfig, FilteredKnnResult, Stream<KnnStatsResult>> {
    private final SimilarityResultStreamDelegate similarityResultStreamDelegate = new SimilarityResultStreamDelegate();
    private final SimilarityStatsProcessor similarityStatsProcessor = new SimilarityStatsProcessor();

    private final boolean shouldComputeSimilarityDistribution;

    FilteredKnnResultBuilderForStatsMode(boolean shouldComputeSimilarityDistribution) {
        this.shouldComputeSimilarityDistribution = shouldComputeSimilarityDistribution;
    }

    @Override
    public Stream<KnnStatsResult> build(
        Graph graph,
        FilteredKnnStatsConfig configuration,
        Optional<FilteredKnnResult> result,
        AlgorithmProcessingTimings timings
    ) {
        var configurationMap = configuration.toMap();

        if (result.isEmpty()) return Stream.of(KnnStatsResult.emptyFrom(
            timings,
            configurationMap
        ));

        var filteredKnnResult = result.get();

        var similarityGraphResult = similarityResultStreamDelegate.computeSimilarityGraph(
            graph,
            configuration.concurrency(),
            filteredKnnResult.similarityResultStream()
        );
        var communityStatistics = similarityStatsProcessor.computeSimilarityStatistics(
            similarityGraphResult,
            shouldComputeSimilarityDistribution
        );
        var similarityDistribution = similarityStatsProcessor.computeSimilarityDistribution(
            shouldComputeSimilarityDistribution,
            similarityGraphResult
        );

        return Stream.of(
            new KnnStatsResult(
                timings.preProcessingMillis,
                timings.computeMillis,
                communityStatistics.computeMilliseconds(),
                filteredKnnResult.nodesCompared(),
                filteredKnnResult.numberOfSimilarityPairs(),
                similarityDistribution,
                filteredKnnResult.didConverge(),
                filteredKnnResult.ranIterations(),
                filteredKnnResult.nodePairsConsidered(),
                configurationMap
            )
        );
    }
}
