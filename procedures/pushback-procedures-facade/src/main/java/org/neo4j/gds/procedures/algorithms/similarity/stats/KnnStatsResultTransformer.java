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
package org.neo4j.gds.procedures.algorithms.similarity.stats;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.similarity.KnnStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.stream.Stream;

public class KnnStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<KnnResult>, Stream<KnnStatsResult>> {

    private final Graph graph;
    private final boolean shouldComputeSimilarityDistribution;
    private final Map<String, Object> configuration;
    private final TerminationFlag terminationFlag;
    private final Concurrency concurrency;

    public KnnStatsResultTransformer(
        Graph graph,
        boolean shouldComputeSimilarityDistribution,
        Map<String, Object> configuration,
        TerminationFlag terminationFlag,
        Concurrency concurrency
    ) {
        this.graph = graph;
        this.shouldComputeSimilarityDistribution = shouldComputeSimilarityDistribution;
        this.configuration = configuration;
        this.terminationFlag = terminationFlag;
        this.concurrency = concurrency;
    }

    @Override
    public Stream<KnnStatsResult> apply(TimedAlgorithmResult<KnnResult> timedAlgorithmResult) {
        var knnResult = timedAlgorithmResult.result();

        if (knnResult == KnnResult.EMPTY){
            return Stream.of(KnnStatsResult.emptyFrom(
                0,
                timedAlgorithmResult.computeMillis(),
                0,
                configuration
            ));
        }
        var similarityStats = SimilarityStatsTools.computeSimilarityDistribution(
            concurrency,
            knnResult.streamSimilarityResult(),
            shouldComputeSimilarityDistribution,
            terminationFlag
        );


        return Stream.of(
            new KnnStatsResult(
                0,
                timedAlgorithmResult.computeMillis(),
                similarityStats.computeMilliseconds(),
                knnResult.nodesCompared(),
                knnResult.totalSimilarityPairs(),
                similarityStats.distribution(),
                knnResult.didConverge(),
                knnResult.ranIterations(),
                knnResult.nodePairsConsidered(),
                configuration
            )
        );
    }
}
