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
package org.neo4j.gds.paths.singlesource.dijkstra;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.MutateResult;
import org.neo4j.gds.paths.ShortestPathMutateResultConsumer;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.allShortestPaths.dijkstra.mutate", description = Dijkstra.DESCRIPTION_SOURCE_TARGET, executionMode = MUTATE_RELATIONSHIP)
public class AllShortestPathsDijkstraMutateSpec implements AlgorithmSpec<Dijkstra, PathFindingResult, AllShortestPathsDijkstraMutateConfig, Stream<MutateResult>, DijkstraFactory.AllShortestPathsDijkstraFactory<AllShortestPathsDijkstraMutateConfig>> {

    @Override
    public String name() {
        return "gds.allShortestPaths.dijkstra.mutate";
    }

    @Override
    public DijkstraFactory.AllShortestPathsDijkstraFactory<AllShortestPathsDijkstraMutateConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new DijkstraFactory.AllShortestPathsDijkstraFactory<>();
    }

    @Override
    public NewConfigFunction<AllShortestPathsDijkstraMutateConfig> newConfigFunction() {
        return (username, configuration) -> AllShortestPathsDijkstraMutateConfig.of(configuration);
    }

    @Override
    public ComputationResultConsumer<Dijkstra, PathFindingResult, AllShortestPathsDijkstraMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new ShortestPathMutateResultConsumer<>();
    }

@Override
    public boolean releaseProgressTask() {
        return false;
    }
}
