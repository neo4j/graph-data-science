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
package org.neo4j.gds.embeddings.hashgnn;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.embeddings.HashGNNStreamResult;

import java.util.stream.Stream;

import static org.neo4j.gds.embeddings.hashgnn.HashGNNProcCompanion.HASH_GNN_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.hashgnn.stream", aliases = {"gds.beta.hashgnn.stream"}, description = HASH_GNN_DESCRIPTION, executionMode = STREAM)
public class HashGNNStreamSpec implements AlgorithmSpec<HashGNN, HashGNNResult, HashGNNStreamConfig, Stream<HashGNNStreamResult>, HashGNNFactory<HashGNNStreamConfig>> {

    @Override
    public String name() {
        return "HashGNNStream";
    }

    @Override
    public HashGNNFactory<HashGNNStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new HashGNNFactory<>();
    }

    @Override
    public NewConfigFunction<HashGNNStreamConfig> newConfigFunction() {
        return (___,config) -> HashGNNStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<HashGNN, HashGNNResult, HashGNNStreamConfig, Stream<HashGNNStreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
