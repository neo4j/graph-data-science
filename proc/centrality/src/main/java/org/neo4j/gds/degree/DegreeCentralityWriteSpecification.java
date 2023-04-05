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
package org.neo4j.gds.degree;

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.degree.DegreeCentrality.DEGREE_CENTRALITY_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;

@GdsCallable(name = "gds.degree.write", description = DEGREE_CENTRALITY_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class DegreeCentralityWriteSpecification implements AlgorithmSpec<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityWriteConfig, Stream<WriteResult>, DegreeCentralityFactory<DegreeCentralityWriteConfig>> {
    @Override
    public String name() {
        return "DegreeCentralityWrite";
    }

    @Override
    public DegreeCentralityFactory<DegreeCentralityWriteConfig> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<DegreeCentralityWriteConfig> newConfigFunction() {
        return (__, userInput) -> DegreeCentralityWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().writeProperty(),
                DegreeCentralityNodePropertyValues.from(computationResult)
            )),
            name()
        );
    }

    private AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityWriteConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new WriteResult.Builder(
            executionContext.returnColumns(),
            computationResult.config().concurrency()
        );

        Optional.ofNullable(computationResult.result())
            .ifPresent(result -> builder.withCentralityFunction(result::get));

        return builder;
    }
}
