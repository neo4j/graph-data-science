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
package org.neo4j.graphalgo.beta.paths.dijkstra;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.beta.paths.PathResult;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.beta.paths.dijkstra.DijkstraProc.DIJKSTRA_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class DijkstraStreamProc extends StreamProc<Dijkstra, DijkstraResult, PathResult, DijkstraStreamConfig> {

    @Procedure(name = "gds.beta.shortestPath.dijkstra.stream", mode = READ)
    @Description(DIJKSTRA_DESCRIPTION)
    public Stream<PathResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Override
    protected Stream<PathResult> stream(ComputationResult<Dijkstra, DijkstraResult, DijkstraStreamConfig> computationResult) {
        return runWithExceptionLogging("Result streaming failed", () -> {
            var graph = computationResult.graph();

            if (computationResult.isGraphEmpty()) {
                graph.release();
                return Stream.empty();
            }
            return computationResult.result().paths()
                .takeWhile(path -> path != PathResult.EMPTY)
                .peek(pathResult -> {
                    pathResult.sourceNode = graph.toOriginalNodeId(pathResult.sourceNode);
                    pathResult.targetNode = graph.toOriginalNodeId(pathResult.targetNode);
                    List<Long> nodeIds = pathResult.nodeIds;
                    for (int i = 0; i < nodeIds.size(); i++) {
                        nodeIds.set(i, graph.toOriginalNodeId(nodeIds.get(i)));
                    }
                });
        });
    }

    @Override
    protected PathResult streamResult(
        long originalNodeId, long internalNodeId, NodeProperties nodeProperties
    ) {
        throw new UnsupportedOperationException("Dijkstra handles result building individually.");
    }

    @Override
    protected DijkstraStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return DijkstraStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Dijkstra, DijkstraStreamConfig> algorithmFactory() {
        return new DijkstraFactory<>();
    }

}
