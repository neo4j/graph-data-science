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
package org.neo4j.gds.paths.singlesource.delta;

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.paths.ShortestPathWriteResultConsumer;
import org.neo4j.gds.paths.delta.DeltaStepping;
import org.neo4j.gds.paths.delta.DeltaSteppingFactory;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;

@GdsCallable(name = "gds.allShortestPaths.delta.write", description = DeltaStepping.DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class AllShortestPathsDeltaWriteSpec implements AlgorithmSpec<DeltaStepping, PathFindingResult, AllShortestPathsDeltaWriteConfig, Stream<StandardWriteRelationshipsResult>, DeltaSteppingFactory<AllShortestPathsDeltaWriteConfig>> {
    @Override
    public String name() {
        return "gds.allShortestPaths.delta.write";
    }

    @Override
    public DeltaSteppingFactory<AllShortestPathsDeltaWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new DeltaSteppingFactory<>();
    }

    @Override
    public NewConfigFunction<AllShortestPathsDeltaWriteConfig> newConfigFunction() {
        return (username, configuration) -> AllShortestPathsDeltaWriteConfig.of(configuration);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ComputationResultConsumer<DeltaStepping, PathFindingResult, AllShortestPathsDeltaWriteConfig, Stream<StandardWriteRelationshipsResult>> computationResultConsumer() {
        return new ShortestPathWriteResultConsumer<>();
    }

@Override
    public boolean releaseProgressTask() {
        return false;
    }
}
