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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.scc.Scc;
import org.neo4j.gds.impl.scc.SccAlgorithmFactory;
import org.neo4j.gds.impl.scc.SccWriteConfig;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

import static org.neo4j.gds.impl.scc.Scc.SCC_DESCRIPTION;

@GdsCallable(name = "gds.alpha.scc.write", description = SCC_DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class SccWriteSpec implements AlgorithmSpec<Scc, HugeLongArray, SccWriteConfig, Stream<WriteResult>, SccAlgorithmFactory<SccWriteConfig>> {

    @Override
    public String name() {
        return "SccWrite";
    }

    @Override
    public SccAlgorithmFactory<SccWriteConfig> algorithmFactory() {
        return new SccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SccWriteConfig> newConfigFunction() {
        return (__, config) -> SccWriteConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Scc, HugeLongArray, SccWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Scc algorithm = computationResult.algorithm();
            HugeLongArray components = computationResult.result();
            var config = computationResult.config();
            Graph graph = computationResult.graph();

            AbstractResultBuilder<WriteResult> writeBuilder = new WriteResult.Builder(
                executionContext.returnColumns(),
                config.concurrency()
            )
                .buildCommunityCount(true)
                .buildHistogram(true)
                .withCommunityFunction(components != null ? components::get : null)
                .withNodeCount(graph.nodeCount())
                .withConfig(config)
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis());

            if (graph.isEmpty()) {
                return Stream.of(writeBuilder.build());
            }

            try (ProgressTimer ignored = ProgressTimer.start(writeBuilder::withWriteMillis)) {
                var progressTracker = new TaskProgressTracker(
                    NodePropertyExporter.baseTask("Scc", graph.nodeCount()),
                    executionContext.log(),
                    config.writeConcurrency(),
                    executionContext.taskRegistryFactory()
                );
                NodePropertyExporter exporter = executionContext.nodePropertyExporterBuilder()
                    .withIdMap(graph)
                    .withTerminationFlag(algorithm.getTerminationFlag())
                    .withProgressTracker(progressTracker)
                    .parallel(Pools.DEFAULT, config.writeConcurrency())
                    .build();

                var properties = components.asNodeProperties();

                exporter.write(
                    config.writeProperty(),
                    properties
                );

                writeBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
            }

            return Stream.of(writeBuilder.build());
        };
    }
}
