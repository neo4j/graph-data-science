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
package org.neo4j.gds.beta.k1coloring;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;

@GdsCallable(name = "gds.beta.k1coloring.write", description = K1ColoringProc.K1_COLORING_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class K1ColoringWriteSpecification implements AlgorithmSpec<K1Coloring, HugeLongArray, K1ColoringWriteConfig, Stream<K1ColoringWriteResult>, K1ColoringFactory<K1ColoringWriteConfig>> {

    @Override
    public String name() {
        return "K1ColoringWrite";
    }

    @Override
    public K1ColoringFactory<K1ColoringWriteConfig> algorithmFactory() {
        return new K1ColoringFactory<>();
    }

    @Override
    public NewConfigFunction<K1ColoringWriteConfig> newConfigFunction() {
        return (__, userInput) -> K1ColoringWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<K1Coloring, HugeLongArray, K1ColoringWriteConfig, Stream<K1ColoringWriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            this::nodePropertyList,
            name()
        );
    }

    private AbstractResultBuilder<K1ColoringWriteResult> resultBuilder(
        ComputationResult<K1Coloring, HugeLongArray, K1ColoringWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        var returnColumns = executionContext.returnColumns();
        var builder = new K1ColoringWriteResult.Builder(returnColumns, computeResult.config().concurrency());
        return K1ColoringProc.resultBuilder(builder, computeResult, returnColumns);
    }

    private List<NodeProperty> nodePropertyList(ComputationResult<K1Coloring, HugeLongArray, K1ColoringWriteConfig> computationResult) {
        var config = computationResult.config();
        var properties = (NodePropertyValues) CommunityProcCompanion.considerSizeFilter(
            config,
            computationResult.result()
                .map(HugeLongArray::asNodeProperties)
                .orElse(EmptyLongNodePropertyValues.INSTANCE)
        );
        return List.of(ImmutableNodeProperty.of(config.writeProperty(), properties));
    }
}
