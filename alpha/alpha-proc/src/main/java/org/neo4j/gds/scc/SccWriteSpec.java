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
package org.neo4j.gds.scc;

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.scc.Scc;
import org.neo4j.gds.impl.scc.SccAlgorithmFactory;
import org.neo4j.gds.impl.scc.SccWriteConfig;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.impl.scc.Scc.SCC_DESCRIPTION;

@GdsCallable(name = "gds.alpha.scc.write", description = SCC_DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class SccWriteSpec implements AlgorithmSpec<Scc, HugeLongArray, SccWriteConfig, Stream<WriteResult>, SccAlgorithmFactory<SccWriteConfig>> {

    @Override
    public String name() {
        return "SccWrite";
    }

    @Override
    public SccAlgorithmFactory<SccWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new SccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SccWriteConfig> newConfigFunction() {
        return (__, config) -> SccWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Scc, HugeLongArray, SccWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().writeProperty(),
                computationResult.result().get().asNodeProperties()
            )),
            name()
        );
    }

    private AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<Scc, HugeLongArray, SccWriteConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var config = computationResult.config();
        var writeBuilder = new WriteResult.Builder(
            executionContext.returnColumns(),
            config.concurrency()
        )
            .buildCommunityCount(true)
            .buildHistogram(true);

        computationResult.result().ifPresent(result -> writeBuilder.withCommunityFunction(result::get));
        
        writeBuilder
            .withNodeCount(computationResult.graph().nodeCount())
            .withConfig(config)
            .withPreProcessingMillis(computationResult.preProcessingMillis())
            .withComputeMillis(computationResult.computeMillis());

        return writeBuilder;
    }
}
