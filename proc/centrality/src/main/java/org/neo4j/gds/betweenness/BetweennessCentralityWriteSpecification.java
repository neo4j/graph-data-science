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
package org.neo4j.gds.betweenness;

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeArrayToNodeProperties;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.betweenness.BetweennessCentrality.BETWEENNESS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;

@GdsCallable(name = "gds.betweenness.write", description = BETWEENNESS_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class BetweennessCentralityWriteSpecification implements AlgorithmSpec<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityWriteConfig, Stream<WriteResult>, BetweennessCentralityFactory<BetweennessCentralityWriteConfig>> {
    @Override
    public String name() {
        return "BetweennessCentralityWrite";
    }

    @Override
    public BetweennessCentralityFactory<BetweennessCentralityWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new BetweennessCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<BetweennessCentralityWriteConfig> newConfigFunction() {
        return (__, userInput) -> BetweennessCentralityWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().writeProperty(),
                HugeArrayToNodeProperties.convert(computationResult.result()
                    .orElseGet(() -> HugeAtomicDoubleArray.of(0, ParallelDoublePageCreator.passThrough(1)))
                )
            )),
            name()
        );
    }

    @Override
    public ValidationConfiguration<BetweennessCentralityWriteConfig> validationConfig(ExecutionContext executionContext) {
        return new BetweennessCentralityConfigValidation<>();
    }

    private AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityWriteConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new WriteResult.Builder(
            executionContext.returnColumns(),
            computationResult.config().concurrency()
        );

        computationResult.result().ifPresent(result -> builder.withCentralityFunction(result::get));

        return builder;
    }

}
