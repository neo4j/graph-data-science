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
import org.neo4j.gds.paths.ShortestPathWriteResultConsumer;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.YensFactory;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;
import static org.neo4j.gds.paths.sourcetarget.YensConstants.DESCRIPTION;

@GdsCallable(name = "gds.shortestPath.yens.write", description = DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class ShortestPathYensWriteSpec implements AlgorithmSpec<Yens, DijkstraResult, ShortestPathYensWriteConfig, Stream<StandardWriteRelationshipsResult>, YensFactory<ShortestPathYensWriteConfig>> {
    @Override
    public String name() {
        return "YensWrite";
    }

    @Override
    public YensFactory<ShortestPathYensWriteConfig> algorithmFactory() {
        return new YensFactory<>();
    }

    @Override
    public NewConfigFunction<ShortestPathYensWriteConfig> newConfigFunction() {
        return (___, config) -> ShortestPathYensWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Yens, DijkstraResult, ShortestPathYensWriteConfig, Stream<StandardWriteRelationshipsResult>> computationResultConsumer() {
        return new ShortestPathWriteResultConsumer<>();
    }

    @Override
    public boolean releaseProgressTask() {
        return false;
    }
}
