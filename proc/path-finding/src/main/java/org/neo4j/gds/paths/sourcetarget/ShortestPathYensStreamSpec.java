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
import org.neo4j.gds.paths.ShortestPathStreamResultConsumer;
import org.neo4j.gds.paths.StreamResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.YensFactory;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.shortestPath.yens.stream", description = YensConstants.DESCRIPTION, executionMode = STREAM)
public class ShortestPathYensStreamSpec implements AlgorithmSpec<Yens, PathFindingResult, ShortestPathYensStreamConfig, Stream<StreamResult>, YensFactory<ShortestPathYensStreamConfig>> {

    @Override
    public String name() {
        return "YensStream";
    }

    @Override
    public YensFactory<ShortestPathYensStreamConfig> algorithmFactory() {
        return new YensFactory<>();
    }

    @Override
    public NewConfigFunction<ShortestPathYensStreamConfig> newConfigFunction() {
        return (___, config) -> ShortestPathYensStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Yens, PathFindingResult, ShortestPathYensStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return new ShortestPathStreamResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
