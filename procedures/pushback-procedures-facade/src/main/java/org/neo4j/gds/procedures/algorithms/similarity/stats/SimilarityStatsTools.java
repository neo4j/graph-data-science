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

import org.neo4j.gds.algorithms.similarity.ActualSimilaritySummaryBuilder;
import org.neo4j.gds.algorithms.similarity.SimilarityResultStreamDelegate;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.result.SimilarityStatistics;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

final class SimilarityStatsTools {

    private SimilarityStatsTools() {}

    private static final SimilarityStatistics.SimilarityDistributionResults EMPTY = new SimilarityStatistics.SimilarityDistributionResults(
        0,
        Map.of(),
        0
    );

    static SimilarityStatistics.SimilarityDistributionResults computeSimilarityDistribution(
        Concurrency concurrency,
        Stream<SimilarityResult> similarityResultStream,
        boolean shouldComputeSimilarityDistribution,
        TerminationFlag terminationFlag
    ) {
        if (!shouldComputeSimilarityDistribution) return EMPTY;

        var statsMillis = new AtomicLong();
        Map<String,Object> distribution;
        var similaritySummaryBuilder  =  ActualSimilaritySummaryBuilder.create(concurrency);
        LongAdder adder = new LongAdder();

        RelationshipWithPropertyConsumer relationshipWithPropertyConsumer= (s,t,w)->{
            adder.increment();
            similaritySummaryBuilder.accept(s,t,w);
            return true;
        };
        var similarityResultStreamDelegate = new SimilarityResultStreamDelegate();
        try (var ignored = ProgressTimer.start(statsMillis::set)) {
            similarityResultStreamDelegate.consumeStream(
                concurrency,
                similarityResultStream,
                terminationFlag,
                relationshipWithPropertyConsumer
            );

            distribution = similaritySummaryBuilder.similaritySummary();
        }

        return new SimilarityStatistics.SimilarityDistributionResults(
            adder.longValue(),
            distribution,
            statsMillis.get()
        );
    }


}
