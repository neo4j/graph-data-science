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
package org.neo4j.gds.kcore;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.kcore.KCoreDecomposition.KCORE_DESCRIPTION;

@GdsCallable(name = "gds.kcore.mutate", description = KCORE_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class KCoreDecompositionMutateSpec implements AlgorithmSpec<KCoreDecomposition, KCoreDecompositionResult, KCoreDecompositionMutateConfig, Stream<MutateResult>, KCoreDecompositionAlgorithmFactory<KCoreDecompositionMutateConfig>> {
    @Override
    public String name() {
        return "KCoreMutate";
    }

    @Override
    public KCoreDecompositionAlgorithmFactory<KCoreDecompositionMutateConfig> algorithmFactory() {
        return new KCoreDecompositionAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KCoreDecompositionMutateConfig> newConfigFunction() {
        return (__, userInput) -> KCoreDecompositionMutateConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<KCoreDecomposition, KCoreDecompositionResult, KCoreDecompositionMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().mutateProperty(),
                nodePropertyValues(computationResult))),
            this::resultBuilder
        );

    }

    private NodePropertyValues nodePropertyValues(ComputationResult<KCoreDecomposition, KCoreDecompositionResult, KCoreDecompositionMutateConfig> computationResult) {
        return computationResult.result()
            .map(KCoreDecompositionResult::coreValues)
            .map(IntNodeProperties::new)
            .orElseGet(() -> new IntNodeProperties(HugeIntArray.newArray(0)));
    }

    private static final class IntNodeProperties implements NodePropertyValues {

        private final HugeIntArray array;

        private IntNodeProperties(HugeIntArray array) {this.array = array;}

        @Override
        public ValueType valueType() {
            return ValueType.LONG;
        }

        @Override
        public @Nullable Object getObject(long nodeId) {
            return array.get(nodeId);
        }

        @Override
        public Value value(long nodeId) {
            return Values.longValue(array.get(nodeId));
        }

        @Override
        public long nodeCount() {
            return array.size();
        }

        @Override
        public long longValue(long nodeId) {
            return array.get(nodeId);
        }

        @Override
        public Optional<Integer> dimension() {
            return Optional.empty();
        }
    }

    private AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<KCoreDecomposition, KCoreDecompositionResult, KCoreDecompositionMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new MutateResult.Builder();
        computationResult.result().ifPresent(result -> builder.withDegeneracy(result.degeneracy()));

        return builder;
    }
}
