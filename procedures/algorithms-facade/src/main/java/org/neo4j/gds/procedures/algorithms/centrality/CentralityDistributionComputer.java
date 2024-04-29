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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.result.CentralityStatistics;

import java.util.Map;
import java.util.function.LongToDoubleFunction;

public class CentralityDistributionComputer {
    /**
     * @return centrality distribution and the time it took to compute it
     */
    public Pair<Map<String, Object>, Long> compute(
        IdMap graph,
        LongToDoubleFunction centralityFunction,
        ConcurrencyConfig configuration,
        boolean shouldComputeCentralityDistribution
    ) {
        var centralityStatistics = CentralityStatistics.centralityStatistics(
            graph.nodeCount(),
            centralityFunction,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            shouldComputeCentralityDistribution
        );

        var centralityDistribution = CentralityStatistics.centralitySummary(centralityStatistics.histogram());

        return Pair.of(
            centralityDistribution,
            centralityStatistics.computeMilliseconds()
        );
    }
}
