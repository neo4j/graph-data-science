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
package org.neo4j.gds.shortestpaths;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.msbfs.AllShortestPathsStreamResult;
import org.neo4j.gds.impl.msbfs.MSBFSASPAlgorithm;
import org.neo4j.gds.impl.msbfs.MSBFSAllShortestPaths;
import org.neo4j.gds.impl.msbfs.WeightedAllShortestPaths;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.shortestpaths.AllShortestPathsConstants.DESCRIPTION;

@GdsCallable(name = "gds.alpha.allShortestPaths.stream", description = DESCRIPTION, executionMode = STREAM)
public class AllShortestPathsStreamSpec implements AlgorithmSpec<MSBFSASPAlgorithm, Stream<AllShortestPathsStreamResult>, AllShortestPathsConfig, Stream<AllShortestPathsStreamResult>, AlgorithmFactory<Graph, MSBFSASPAlgorithm, AllShortestPathsConfig>> {
    @Override
    public String name() {
        return "AllShortestPathsStream";
    }

    @Override
    public AlgorithmFactory<Graph, MSBFSASPAlgorithm, AllShortestPathsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new GraphAlgorithmFactory<>() {
            @Override
            public String taskName() {
                return "MSBFSASPAlgorithm";
            }

            @Override
            public MSBFSASPAlgorithm build(
                Graph graph,
                AllShortestPathsConfig configuration,
                ProgressTracker progressTracker
            ) {
                if (configuration.hasRelationshipWeightProperty()) {
                    return new WeightedAllShortestPaths(
                        graph,
                        Pools.DEFAULT,
                        configuration.concurrency()
                    );
                } else {
                    return new MSBFSAllShortestPaths(
                        graph,
                        configuration.concurrency(),
                        Pools.DEFAULT
                    );
                }
            }
        };
    }

    @Override
    public NewConfigFunction<AllShortestPathsConfig> newConfigFunction() {
        return AllShortestPathsConfig::of;
    }

    @Override
    public ComputationResultConsumer<MSBFSASPAlgorithm, Stream<AllShortestPathsStreamResult>, AllShortestPathsConfig, Stream<AllShortestPathsStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> computationResult.result().orElseGet(Stream::empty);
    }
}
