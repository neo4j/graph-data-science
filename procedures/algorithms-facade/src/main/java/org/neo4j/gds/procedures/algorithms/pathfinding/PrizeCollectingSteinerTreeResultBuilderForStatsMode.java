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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.pcst.PCSTStatsConfig;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;

import java.util.Optional;
import java.util.stream.Stream;

class PrizeCollectingSteinerTreeResultBuilderForStatsMode implements StatsResultBuilder<PrizeSteinerTreeResult, Stream<PrizeCollectingSteinerTreeStatsResult>> {
    private final PCSTStatsConfig configuration;

    PrizeCollectingSteinerTreeResultBuilderForStatsMode(PCSTStatsConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public Stream<PrizeCollectingSteinerTreeStatsResult> build(
        Graph graph,
        Optional<PrizeSteinerTreeResult> result,
        AlgorithmProcessingTimings timings
    ) {

        return Stream.of(
            result.map(
            treeResult-> new PrizeCollectingSteinerTreeStatsResult(
                 timings.preProcessingMillis,
                 timings.computeMillis,
                 treeResult.effectiveNodeCount(),
                 treeResult.totalWeight(),
                 treeResult.sumOfPrizes(),
                 configuration.toMap()
             )).orElse(PrizeCollectingSteinerTreeStatsResult.emptyFrom(timings, configuration.toMap()))
        );

    }
}
