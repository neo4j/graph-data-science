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
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.pcst.PCSTWriteConfig;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;

import java.util.Optional;

class PrizeCollectingSteinerTreeResultBuilderForWriteMode implements ResultBuilder<PCSTWriteConfig,PrizeSteinerTreeResult, PrizeCollectingSteinerTreeWriteResult, RelationshipsWritten> {
    private final PCSTWriteConfig configuration;

    PrizeCollectingSteinerTreeResultBuilderForWriteMode(PCSTWriteConfig configuration) {
        this.configuration = configuration;
    }


    @Override
    public PrizeCollectingSteinerTreeWriteResult build(
        Graph graph,
        PCSTWriteConfig pcstWriteConfig,
        Optional<PrizeSteinerTreeResult> result,
        AlgorithmProcessingTimings timings,
        Optional<RelationshipsWritten> relationshipsWritten
    ) {
        return
            result.map(
                treeResult-> new PrizeCollectingSteinerTreeWriteResult(
                    timings.preProcessingMillis,
                    timings.computeMillis,
                    timings.sideEffectMillis,
                    treeResult.effectiveNodeCount(),
                    treeResult.totalWeight(),
                    treeResult.sumOfPrizes(),
                    relationshipsWritten.map(RelationshipsWritten::value).orElse(0L),
                    configuration.toMap()
                )).orElse(PrizeCollectingSteinerTreeWriteResult.emptyFrom(timings, configuration.toMap()));
    }
}