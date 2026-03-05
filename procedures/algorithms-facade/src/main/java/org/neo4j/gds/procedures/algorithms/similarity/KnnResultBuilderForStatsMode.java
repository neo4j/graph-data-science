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
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.KnnStatsConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.stream.Stream;

class KnnResultBuilderForStatsMode implements StatsResultBuilder<KnnResult, Stream<KnnStatsResult>> {
    private final SimilarityStatsProcessor similarityStatsProcessor = new SimilarityStatsProcessor();
    private final TerminationFlag terminationFlag;
    private final KnnStatsConfig configuration;
    private final boolean shouldComputeSimilarityDistribution;

    KnnResultBuilderForStatsMode(
        TerminationFlag terminationFlag,
        KnnStatsConfig configuration,
        boolean shouldComputeSimilarityDistribution
    ) {
        this.terminationFlag = terminationFlag;
        this.configuration = configuration;
        this.shouldComputeSimilarityDistribution = shouldComputeSimilarityDistribution;
    }

    @Override
    public Stream<KnnStatsResult> build(
        Graph graph,
        Optional<KnnResult> result,
        AlgorithmProcessingTimings timings
    ) {
        var configurationMap = configuration.toMap();

        if (result.isEmpty()) return Stream.of(KnnStatsResult.emptyFrom(
            timings,
            configurationMap
        ));

        var knnResult = result.get();


        var similarityStats = similarityStatsProcessor.computeSimilarityDistribution(
            graph,
            configuration,
            knnResult.streamSimilarityResult(),
            shouldComputeSimilarityDistribution,
            terminationFlag
        );

        return Stream.of(
            new KnnStatsResult(
                timings.preProcessingMillis,
                timings.computeMillis,
                similarityStats.computeMilliseconds(),
                knnResult.nodesCompared(),
                knnResult.totalSimilarityPairs(),
                similarityStats.distribution(),
                knnResult.didConverge(),
                knnResult.ranIterations(),
                knnResult.nodePairsConsidered(),
                configurationMap
            )
        );
    }
}
