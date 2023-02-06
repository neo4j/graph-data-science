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
package org.neo4j.gds.leiden;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.AlgorithmSpecProgressTrackerProvider;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.leiden.LeidenStreamProc.DESCRIPTION;


@GdsCallable(name = "gds.beta.leiden.write", description = DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class LeidenWriteSpec implements AlgorithmSpec<Leiden, LeidenResult, LeidenWriteConfig, Stream<WriteResult>, LeidenAlgorithmFactory<LeidenWriteConfig>> {
    @Override
    public String name() {
        return "LeidenWrite";
    }

    @Override
    public LeidenAlgorithmFactory<LeidenWriteConfig> algorithmFactory() {
        return new LeidenAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LeidenWriteConfig> newConfigFunction() {
        return (__, config) -> LeidenWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Leiden, LeidenResult, LeidenWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var leidenResult = computationResult.result();
            var graph = computationResult.graph();
            var builder = new WriteResult.Builder(executionContext.returnColumns(), computationResult.config().concurrency())
                .withLevels(leidenResult.ranLevels())
                .withDidConverge(leidenResult.didConverge())
                .withModularities(Arrays.stream(leidenResult.modularities())
                    .boxed()
                    .collect(Collectors.toList()))
                .withModularity(leidenResult.modularity())
                .withCommunityFunction(leidenResult.communitiesFunction())
                .withNodeCount(graph.nodeCount())
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withConfig(computationResult.config());

            try (ProgressTimer ignore = ProgressTimer.start(builder::withWriteMillis)) {
                var writeConcurrency = computationResult.config().writeConcurrency();
                var algorithm = computationResult.algorithm();
                var config = computationResult.config();

                NodePropertyExporter exporter =  executionContext.nodePropertyExporterBuilder()
                    .withIdMap(graph)
                    .withTerminationFlag(algorithm.getTerminationFlag())
                    .withProgressTracker(AlgorithmSpecProgressTrackerProvider.createProgressTracker(
                        name(),
                        graph.nodeCount(),
                        writeConcurrency,
                        executionContext
                    ))
                    .parallel(Pools.DEFAULT, writeConcurrency)
                    .build();

                var properties = LeidenCompanion.leidenNodeProperties(
                    computationResult,
                    computationResult.config().writeProperty()
                );

                exporter.write(
                    config.writeProperty(),
                    properties
                );
                builder.withNodePropertiesWritten(exporter.propertiesWritten());
            }
            return Stream.of(builder.build());
        };
    }

    @NotNull
    private AbstractResultBuilder<WriteResult> resultBuilder(
        ComputationResult<Leiden, LeidenResult, LeidenWriteConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var leidenResult = computationResult.result();
        return new WriteResult.Builder(executionContext.returnColumns(), computationResult.config().concurrency())
            .withLevels(leidenResult.ranLevels())
            .withDidConverge(leidenResult.didConverge())
            .withModularities(Arrays.stream(leidenResult.modularities())
                .boxed()
                .collect(Collectors.toList()))
            .withModularity(leidenResult.modularity())
            .withCommunityFunction(leidenResult.communitiesFunction())
            .withConfig(computationResult.config());
    }

}
