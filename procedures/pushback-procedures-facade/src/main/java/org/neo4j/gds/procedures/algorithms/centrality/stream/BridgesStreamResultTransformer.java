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
package org.neo4j.gds.procedures.algorithms.centrality.stream;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.procedures.algorithms.centrality.BridgesStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.List;
import java.util.stream.Stream;

public class BridgesStreamResultTransformer implements ResultTransformer<TimedAlgorithmResult<BridgeResult>, Stream<BridgesStreamResult>> {
    private final Graph graph;
    private final boolean shouldComputeComponents;

    public BridgesStreamResultTransformer(
        Graph graph,
        boolean shouldComputeComponents
    ) {
        this.graph = graph;
        this.shouldComputeComponents = shouldComputeComponents;
    }

    @Override
    public Stream<BridgesStreamResult> apply(TimedAlgorithmResult<BridgeResult> timedAlgorithmResult) {

        var result = timedAlgorithmResult.result();
        if (shouldComputeComponents){
            return createWithComponents(result);
        }else{
            return createWithoutComponents(result);
        }
    }

    private Stream<BridgesStreamResult> createWithComponents(BridgeResult result){
        return result
            .bridges()
            .stream()
            .map(bridge -> new BridgesStreamResult(
                graph.toOriginalNodeId(bridge.from()),
                graph.toOriginalNodeId(bridge.to()),
                List.of(bridge.remainingSizes()[0], bridge.remainingSizes()[1])
            ));
    }

    private Stream<BridgesStreamResult> createWithoutComponents(BridgeResult result){
        return result
            .bridges()
            .stream()
            .map(bridge -> new BridgesStreamResult(
                graph.toOriginalNodeId(bridge.from()),
                graph.toOriginalNodeId(bridge.to()),
                null
            ));
    }
}
