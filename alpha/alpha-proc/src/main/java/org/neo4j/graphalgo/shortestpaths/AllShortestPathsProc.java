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
package org.neo4j.graphalgo.shortestpaths;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.msbfs.AllShortestPathsStream;
import org.neo4j.graphalgo.impl.msbfs.MSBFSASPAlgorithm;
import org.neo4j.graphalgo.impl.msbfs.MSBFSAllShortestPaths;
import org.neo4j.graphalgo.impl.msbfs.WeightedAllShortestPaths;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class AllShortestPathsProc extends AlgoBaseProc<MSBFSASPAlgorithm, Stream<AllShortestPathsStream.Result>, AllShortestPathsConfig> {

    private static final String DESCRIPTION = "The All Pairs Shortest Path (APSP) calculates the shortest (weighted) path between all pairs of nodes.";

    @Procedure(name = "gds.alpha.allShortestPaths.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<AllShortestPathsStream.Result> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<MSBFSASPAlgorithm, Stream<AllShortestPathsStream.Result>, AllShortestPathsConfig> computationResult =
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
    protected AlgorithmFactory<MSBFSASPAlgorithm, AllShortestPathsConfig> algorithmFactory() {
        return (AlphaAlgorithmFactory<MSBFSASPAlgorithm, AllShortestPathsConfig>) (graph, configuration, tracker, log, eventTracker) -> {
            if (configuration.hasRelationshipWeightProperty()) {
                return new WeightedAllShortestPaths(
                    graph,
                    Pools.DEFAULT,
                    configuration.concurrency()
                )
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            } else {
                return new MSBFSAllShortestPaths(
                    graph,
                    tracker,
                    configuration.concurrency(),
                    Pools.DEFAULT
                )
                    .withTerminationFlag(TerminationFlag.wrap(transaction));
            }
        };
    }
}
