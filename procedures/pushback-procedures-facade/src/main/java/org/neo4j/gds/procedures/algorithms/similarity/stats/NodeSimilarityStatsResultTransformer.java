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

import org.neo4j.gds.procedures.algorithms.similarity.SimilarityStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.similarity.SimilarityGraph;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.Map;
import java.util.stream.Stream;

public class NodeSimilarityStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<NodeSimilarityResult>, Stream<SimilarityStatsResult>> {

    private final boolean shouldComputeSimilarityDistribution;
    private final Map<String, Object> configuration;

    public NodeSimilarityStatsResultTransformer(
        boolean shouldComputeSimilarityDistribution,
        Map<String, Object> configuration
    ) {
        this.shouldComputeSimilarityDistribution = shouldComputeSimilarityDistribution;
        this.configuration = configuration;
    }

    @Override
    public Stream<SimilarityStatsResult> apply(TimedAlgorithmResult<NodeSimilarityResult> timedAlgorithmResult) {
        var result = timedAlgorithmResult.result();

        SimilarityGraph similarityGraphResult = result.graphResult();
        var similarityStats = SimilarityStatsTools.computeSimilarityDistribution(
            shouldComputeSimilarityDistribution,
            similarityGraphResult
        );

        return Stream.of(
            new SimilarityStatsResult(
                0,
                timedAlgorithmResult.computeMillis(),
                similarityStats.computeMilliseconds(),
                result.comparedNodes(),
                similarityGraphResult.relationshipCount(),
                similarityStats.distribution(),
                configuration
            )
        );

    }
}
