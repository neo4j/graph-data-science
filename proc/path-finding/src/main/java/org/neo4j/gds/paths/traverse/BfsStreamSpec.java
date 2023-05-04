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
package org.neo4j.gds.paths.traverse;

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.paths.traverse.BfsStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.bfs.stream", description = DESCRIPTION, executionMode = STREAM)
public class BfsStreamSpec implements AlgorithmSpec<BFS, HugeLongArray, BfsStreamConfig, Stream<BfsStreamResult>, BfsAlgorithmFactory<BfsStreamConfig>> {
    @Override
    public String name() {
        return "BfsStream";
    }

    @Override
    public BfsAlgorithmFactory<BfsStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new BfsAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<BfsStreamConfig> newConfigFunction() {
        return (__, config) -> BfsStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<BFS, HugeLongArray, BfsStreamConfig, Stream<BfsStreamResult>> computationResultConsumer() {
        return new BfsStreamComputationResultConsumer(new PathFactoryFacade());
    }

}
