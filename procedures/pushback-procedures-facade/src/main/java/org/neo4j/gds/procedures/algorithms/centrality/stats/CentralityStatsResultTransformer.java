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
package org.neo4j.gds.procedures.algorithms.centrality.stats;

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityDistributionHelpers;
import org.neo4j.gds.procedures.algorithms.centrality.CentralityStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class CentralityStatsResultTransformer<R extends CentralityAlgorithmResult> implements ResultTransformer<TimedAlgorithmResult<R>, Stream<CentralityStatsResult>> {

    private final Graph graph;
    private final Map<String, Object> configuration;
    private final boolean shouldComputeDistribution;
    private final Concurrency concurrency;

    public CentralityStatsResultTransformer(
        Graph graph,
        Map<String, Object> configuration,
        boolean shouldComputeDistribution,
        Concurrency concurrency
    ) {
        this.graph = graph;
        this.configuration = configuration;
        this.shouldComputeDistribution = shouldComputeDistribution;
        this.concurrency = concurrency;
    }

    @Override
    public Stream<CentralityStatsResult> apply(TimedAlgorithmResult<R> timedAlgorithmResult) {
        var centralityAlgorithmResult = timedAlgorithmResult.result();

        var centralityDistribution = CentralityDistributionHelpers.compute(
            graph,
            centralityAlgorithmResult.centralityScoreProvider(),
            concurrency,
            shouldComputeDistribution
        );

        return
            Stream.of(
                new CentralityStatsResult(
                    centralityDistribution.centralitySummary(),
                    0,
                    timedAlgorithmResult.computeMillis(),
                    centralityDistribution.computeMillis(),
                    configuration
                )
            );
    }
}
