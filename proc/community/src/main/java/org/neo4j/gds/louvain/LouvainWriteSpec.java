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
package org.neo4j.gds.louvain;

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.louvain.LouvainConstants.DESCRIPTION;

@GdsCallable(name = "gds.louvain.write", description = DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class LouvainWriteSpec implements AlgorithmSpec<Louvain, LouvainResult, LouvainWriteConfig, Stream<WriteResult>, LouvainAlgorithmFactory<LouvainWriteConfig>> {
    @Override
    public String name() {
        return "LouvainWrite";
    }

    @Override
    public LouvainAlgorithmFactory<LouvainWriteConfig> algorithmFactory() {
        return new LouvainAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LouvainWriteConfig> newConfigFunction() {
        return (__, config) -> LouvainWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Louvain, LouvainResult, LouvainWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            LouvainResultBuilder::createForWrite,
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().writeProperty(),
                nodeProperties(computationResult)
            )),
            name()
        );
    }

    @Deprecated
    protected NodePropertyValues nodeProperties(ComputationResult<Louvain, LouvainResult, LouvainWriteConfig> computationResult) {
        return LouvainNodePropertyValuesDelegate.extractNodeProperties(
            computationResult,
            computationResult.config().writeProperty()
        );
    }
}
