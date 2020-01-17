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
package org.neo4j.graphalgo.shortestpaths;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.shortestpaths.ShortestPathAStar;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ShortestPathAStarProc extends AlgoBaseProc<ShortestPathAStar, ShortestPathAStar, ShortestPathAStarConfig> {

    @Procedure(name = "gds.alpha.shortestPath.astar.stream", mode = READ)
    public Stream<ShortestPathAStar.Result> astarStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ShortestPathAStar, ShortestPathAStar, ShortestPathAStarConfig> computationResult = compute(
            graphNameOrConfig,
            configuration,
            false,
            false
        );

        Graph graph = computationResult.graph();
        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        ShortestPathAStar algo = computationResult.algorithm();
        return algo.resultStream();
    }

    @Override
    protected void validateGraphCreateConfig(
        GraphCreateConfig graphCreateConfig,
        ShortestPathAStarConfig config
    ) {
        if (config.direction() == Direction.BOTH) {
            graphCreateConfig.relationshipProjection().projections().entrySet().stream()
                .filter(entry -> entry.getValue().projection() != Projection.UNDIRECTED)
                .forEach(entry -> {
                    throw new IllegalArgumentException(String.format(
                        "Procedure requires relationship projections to be UNDIRECTED. Projection for `%s` uses projection `%s`",
                        entry.getKey().name,
                        entry.getValue().projection()
                    ));
                });
        }
    }

    @Override
    protected ShortestPathAStarConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ShortestPathAStarConfig.of(graphName, maybeImplicitCreate, username, config);
    }

    @Override
    protected AlgorithmFactory<ShortestPathAStar, ShortestPathAStarConfig> algorithmFactory(ShortestPathAStarConfig config) {
        return new AlphaAlgorithmFactory<ShortestPathAStar, ShortestPathAStarConfig>() {
            @Override
            public ShortestPathAStar build(
                Graph graph, ShortestPathAStarConfig configuration, AllocationTracker tracker, Log log
            ) {
                return new ShortestPathAStar(
                    graph,
                    api,
                    configuration.startNodeId(),
                    configuration.endNodeId(),
                    configuration.propertyKeyLat(),
                    configuration.propertyKeyLon(),
                    configuration.resolvedDirection()
                );
            }
        };
    }
}
