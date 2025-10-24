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
package org.neo4j.gds.procedures.algorithms.community.stream;

import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.community.SccStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public final class SccStreamTransformer implements ResultTransformer<TimedAlgorithmResult<HugeLongArray>, Stream<SccStreamResult>> {
    private final Graph graph;
    private final Concurrency concurrency;
    private final boolean consecutiveIds;

    public SccStreamTransformer(
        Graph graph,
        Concurrency concurrency,
        boolean consecutiveIds
    ) {
        this.graph = graph;
        this.concurrency = concurrency;
        this.consecutiveIds = consecutiveIds;
    }

    @Override
    public Stream<SccStreamResult> apply(
        TimedAlgorithmResult<HugeLongArray> timedAlgorithmResult
    ) {
        var sccResult = timedAlgorithmResult.result();

        var nodePropertyValues = CommunityCompanion.nodePropertyValues(
            consecutiveIds,
            NodePropertyValuesAdapter.adapt(sccResult),
            Optional.empty(),
            concurrency
        );

        return LongStream
            .range(IdMap.START_NODE_ID, nodePropertyValues.nodeCount())
            .filter(nodePropertyValues::hasValue)
            .mapToObj(nodeId -> new SccStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    nodePropertyValues.longValue(nodeId)
                )
            );
    }
}
