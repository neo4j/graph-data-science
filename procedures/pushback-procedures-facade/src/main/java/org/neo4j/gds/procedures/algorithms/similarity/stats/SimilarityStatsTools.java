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

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.SimilarityStatistics;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.stream.Stream;

final class SimilarityStatsTools {

    private SimilarityStatsTools() {}

     static SimilarityStatistics.SimilarityStats computeSimilarityStatistics(
        IdMap graph,
        Concurrency concurrency,
        Stream<SimilarityResult> similarityResultStream,
        boolean shouldComputeSimilarityDistribution,
        TerminationFlag terminationFlag
    ) {
        if (!shouldComputeSimilarityDistribution) {
            return computeSimilarityStatistics(SimilarityGraphResult.empty(), false);
        }
        var similarityGraph = SimilarityGraphResult.fromStream(
                graph,
                concurrency,
                similarityResultStream,
                terminationFlag
        );
        return computeSimilarityStatistics(similarityGraph,true);

    }

    static SimilarityStatistics.SimilarityStats computeSimilarityStatistics(
        SimilarityGraphResult similarityGraphResult,
        boolean shouldComputeSimilarityDistribution
    ) {
        return SimilarityStatistics.similarityStats(
            similarityGraphResult::similarityGraph,
            shouldComputeSimilarityDistribution
        );
    }
}
