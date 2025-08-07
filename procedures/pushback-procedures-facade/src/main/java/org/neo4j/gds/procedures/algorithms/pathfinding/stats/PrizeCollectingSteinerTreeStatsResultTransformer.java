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
package org.neo4j.gds.procedures.algorithms.pathfinding.stats;

import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.PrizeCollectingSteinerTreeStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

class PrizeCollectingSteinerTreeStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<PrizeSteinerTreeResult>, Stream<PrizeCollectingSteinerTreeStatsResult>> {

    private final Map<String, Object> configuration;

    PrizeCollectingSteinerTreeStatsResultTransformer(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public Stream<PrizeCollectingSteinerTreeStatsResult> apply(TimedAlgorithmResult<PrizeSteinerTreeResult> algorithmResult) {
        var result = algorithmResult.result();
        var statsResult = new PrizeCollectingSteinerTreeStatsResult(
            0,
            algorithmResult.computeMillis(),
            result.effectiveNodeCount(),
            result.totalWeight(),
            result.sumOfPrizes(),
            configuration
        );

        return Stream.of(statsResult);
    }
}
