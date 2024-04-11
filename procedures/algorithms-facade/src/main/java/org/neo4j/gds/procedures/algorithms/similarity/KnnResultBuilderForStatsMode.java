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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.result.SimilarityStatistics;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.KnnStatsConfig;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

class KnnResultBuilderForStatsMode implements ResultBuilder<KnnStatsConfig, KnnResult, Stream<KnnStatsResult>, Void> {
    private final boolean shouldComputeSimilarityDistribution;

    KnnResultBuilderForStatsMode(boolean shouldComputeSimilarityDistribution) {
        this.shouldComputeSimilarityDistribution = shouldComputeSimilarityDistribution;
    }

    @Override
    public Stream<KnnStatsResult> build(
        Graph graph,
        GraphStore graphStore,
        KnnStatsConfig configuration,
        Optional<KnnResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Void> unused
    ) {
        //noinspection OptionalIsPresent
        if (result.isEmpty()) return Stream.of(new KnnStatsResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.postProcessingMillis,
            0,
            0,
            Collections.emptyMap(),
            false,
            0,
            0,
            configuration.toMap()
        ));

        var similarityGraphResult = SimilarityResultCompanion.computeToGraph(
            graph,
            graph.nodeCount(),
            configuration.typedConcurrency(),
            result.get().streamSimilarityResult()
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
                result.get().nodesCompared(),
                result.get().totalSimilarityPairs(),
                similaritySummary,
                result.get().didConverge(),
                result.get().ranIterations(),
                result.get().nodePairsConsidered(),
                configuration.toMap()
            )
        );
    }
}
