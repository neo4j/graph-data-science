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
package org.neo4j.gds.paths.sourcetarget;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.MutateResult;
import org.neo4j.gds.paths.ShortestPathMutateResultConsumer;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.YensFactory;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.paths.sourcetarget.YensConstants.DESCRIPTION;

@GdsCallable(name = "gds.shortestPath.yens.mutate", description = DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class ShortestPathYensMutateSpec implements AlgorithmSpec<Yens, PathFindingResult, ShortestPathYensMutateConfig, Stream<MutateResult>, YensFactory<ShortestPathYensMutateConfig>> {
    @Override
    public String name() {
        return "YensMutate";
    }

    @Override
    public YensFactory<ShortestPathYensMutateConfig> algorithmFactory() {
        return new YensFactory<>();
    }

    @Override
    public NewConfigFunction<ShortestPathYensMutateConfig> newConfigFunction() {
        return (___, config) -> ShortestPathYensMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Yens, PathFindingResult, ShortestPathYensMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new ShortestPathMutateResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
