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
import org.neo4j.gds.paths.MutateResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.bfs.mutate", description = BfsStreamProc.DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class BfsMutateSpec implements AlgorithmSpec<BFS, HugeLongArray, BfsMutateConfig, Stream<MutateResult>, BfsAlgorithmFactory<BfsMutateConfig>> {
    @Override
    public String name() {
        return "gds.bfs.mutate";
    }

    @Override
    public BfsAlgorithmFactory<BfsMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new BfsAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<BfsMutateConfig> newConfigFunction() {
        return (__, config) -> BfsMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<BFS, HugeLongArray, BfsMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new BfsMutateComputationResultConsumer();
    }
}
