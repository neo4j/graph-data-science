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

import com.carrotsearch.hppc.IntDoubleMap;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.Translators;
import org.neo4j.graphalgo.impl.shortestpaths.ShortestPaths;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.results.ShortestPathResult;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ShortestPathsProc extends AlgoBaseProc<ShortestPaths, ShortestPaths, ShortestPathsConfig> {

    private static final String DESCRIPTION = "The Shortest Path algorithm calculates the shortest (weighted) path between a pair of nodes.";

    @Procedure(name = "gds.alpha.shortestPaths.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<ShortestPaths.Result> dijkstraStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ShortestPaths, ShortestPaths, ShortestPathsConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        if (computationResult.graph().isEmpty()) {
            return Stream.empty();
        }

        return computationResult.algorithm().resultStream();
    }

    @Procedure(value = "gds.alpha.shortestPaths.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<ShortestPathResult> dijkstra(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<ShortestPaths, ShortestPaths, ShortestPathsConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        final ShortestPaths algorithm = computationResult.algorithm();

        ShortestPathResult.Builder builder = ShortestPathResult.builder();
        try(ProgressTimer ignore = ProgressTimer.start(builder::withWriteMillis)) {
            IntDoubleMap shortestPaths = algorithm.getShortestPaths();
            algorithm.release();

            ShortestPathsConfig config = computationResult.config();
            NodePropertyExporter.of(api, computationResult.graph(), algorithm.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build()
                .write(
                    config.writeProperty(),
                    shortestPaths,
                    Translators.INT_DOUBLE_MAP_TRANSLATOR
                );
        }

        return Stream.of(builder.build());
    }

    @Override
    protected ShortestPathsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ShortestPathsConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<ShortestPaths, ShortestPathsConfig> algorithmFactory(ShortestPathsConfig config) {
        return new AlphaAlgorithmFactory<ShortestPaths, ShortestPathsConfig>() {
            @Override
            public ShortestPaths build(
                Graph graph,
                ShortestPathsConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                return new ShortestPaths(graph, config.startNode());
            }
        };
    }
}
