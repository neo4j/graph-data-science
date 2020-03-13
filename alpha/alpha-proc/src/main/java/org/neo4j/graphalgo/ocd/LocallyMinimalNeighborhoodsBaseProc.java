/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.ocd;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.ocd.lhs.LocallyMinimalNeighborhoods;
import org.neo4j.graphalgo.impl.ocd.lhs.LocallyMinimalNeighborhoodsBaseConfig;
import org.neo4j.logging.Log;

public abstract class LocallyMinimalNeighborhoodsBaseProc<CONFIG extends LocallyMinimalNeighborhoodsBaseConfig>
    extends AlgoBaseProc<LocallyMinimalNeighborhoods, LocallyMinimalNeighborhoods.Result, CONFIG>  {

    static final String DESCRIPTION =
        "LocallyMinimalNeighborhoods is an overlapping community detection graph algorithm that is used to " +
        "find good seeding communities for other community detection algorithms.";

    @Override
    protected void validateConfigs(GraphCreateConfig graphCreateConfig, CONFIG config) {
        graphCreateConfig.relationshipProjections().projections().entrySet().stream()
            .filter(entry -> entry.getValue().orientation() != Orientation.UNDIRECTED)
            .forEach(entry -> {
                throw new IllegalArgumentException(String.format(
                    "Procedure requires relationship projections to be UNDIRECTED. Projection for `%s` uses projection `%s`",
                    entry.getKey().name,
                    entry.getValue().orientation()
                ));
            });
    }

    @Override
    protected AlgorithmFactory<LocallyMinimalNeighborhoods, CONFIG> algorithmFactory(
        LocallyMinimalNeighborhoodsBaseConfig config
    ) {
        return new AlphaAlgorithmFactory<LocallyMinimalNeighborhoods, CONFIG>() {
            @Override
            public LocallyMinimalNeighborhoods build(
                Graph graph,
                LocallyMinimalNeighborhoodsBaseConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new LocallyMinimalNeighborhoods(
                    graph,
                    log,
                    transaction,
                    Pools.DEFAULT,
                    tracker,
                    configuration.concurrency(),
                    configuration.includeMembers()
                )
                    .withProgressLogger(ProgressLogger.wrap(log, "TriangleCount"))
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }

}
