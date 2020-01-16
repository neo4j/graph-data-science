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
package org.neo4j.graphalgo.shortestpath;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.msbfs.MSBFSASPAlgorithm;
import org.neo4j.graphalgo.impl.msbfs.MSBFSAllShortestPaths;
import org.neo4j.graphalgo.impl.msbfs.WeightedAllShortestPaths;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class AllShortestPathsProc extends AlgoBaseProc<MSBFSASPAlgorithm, Stream<WeightedAllShortestPaths.Result>, AllShortestPathsConfig> {

    @Procedure(name = "gds.alpha.allShortestPaths.stream", mode = READ)
    public Stream<WeightedAllShortestPaths.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<MSBFSASPAlgorithm, Stream<WeightedAllShortestPaths.Result>, AllShortestPathsConfig> computationResult =
            compute(graphNameOrConfig, configuration, false, false);

        if (computationResult.isGraphEmpty()) {
            computationResult.graph().release();
            return Stream.empty();
        }

        return computationResult.result();
    }

    @Override
    protected AllShortestPathsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return AllShortestPathsConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<MSBFSASPAlgorithm, AllShortestPathsConfig> algorithmFactory(AllShortestPathsConfig config) {
        return new AlphaAlgorithmFactory<MSBFSASPAlgorithm, AllShortestPathsConfig>() {
            @Override
            public MSBFSASPAlgorithm build(
                Graph graph,
                AllShortestPathsConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                if (config.weightProperty() != null) {
                    return new WeightedAllShortestPaths(
                        graph,
                        Pools.DEFAULT,
                        configuration.concurrency(),
                        config.resolvedDirection()
                    )
                        .withProgressLogger(ProgressLogger.wrap(log, "WeightedAllShortestPaths)"))
                        .withTerminationFlag(TerminationFlag.wrap(transaction));
                } else {
                    return new MSBFSAllShortestPaths(
                        graph,
                        tracker,
                        configuration.concurrency(),
                        Pools.DEFAULT,
                        config.resolvedDirection()
                    )
                        .withProgressLogger(ProgressLogger.wrap(log, "AllShortestPaths(MultiSource)"))
                        .withTerminationFlag(TerminationFlag.wrap(transaction));
                }
            }
        };
    }
}
