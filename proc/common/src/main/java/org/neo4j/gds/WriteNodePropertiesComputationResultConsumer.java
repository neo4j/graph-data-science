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
package org.neo4j.gds;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public class WriteNodePropertiesComputationResultConsumer<ALGO extends Algorithm<ALGO_RESULT>, ALGO_RESULT, CONFIG extends WritePropertyConfig & AlgoBaseConfig, RESULT>
    implements ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, Stream<RESULT>> {

    public interface WriteNodePropertyListFunction<ALGO extends Algorithm<ALGO_RESULT>, ALGO_RESULT, CONFIG extends WritePropertyConfig & AlgoBaseConfig>
        extends NodePropertyListFunction<ALGO, ALGO_RESULT, CONFIG> {}

    private final ResultBuilderFunction<ALGO, ALGO_RESULT, CONFIG, RESULT> resultBuilderFunction;
    private final WriteNodePropertyListFunction<ALGO, ALGO_RESULT, CONFIG> nodePropertyListFunction;
    private final String procedureName;

    public WriteNodePropertiesComputationResultConsumer(
        ResultBuilderFunction<ALGO, ALGO_RESULT, CONFIG, RESULT> resultBuilderFunction,
        WriteNodePropertyListFunction<ALGO, ALGO_RESULT, CONFIG> nodePropertyListFunction,
        String procedureName
    ) {
        this.resultBuilderFunction = resultBuilderFunction;
        this.nodePropertyListFunction = nodePropertyListFunction;
        this.procedureName = procedureName;
    }

    @Override
    public Stream<RESULT> consume(
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult, ExecutionContext executionContext
    ) {
        return runWithExceptionLogging("Graph write failed", executionContext.log(), () -> {
            CONFIG config = computationResult.config();

            AbstractResultBuilder<RESULT> builder = resultBuilderFunction.apply(computationResult, executionContext)
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(config);

            if (!computationResult.isGraphEmpty()) {
                writeToNeo(builder, computationResult, executionContext);
            }
            return Stream.of(builder.build());
        });
    }

    void writeToNeo(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
            Graph graph = computationResult.graph();
            var progressTracker = createProgressTracker(
                graph.nodeCount(),
                computationResult.config().writeConcurrency(),
                executionContext
            );
            var exporter = executionContext
                .nodePropertyExporterBuilder()
                .withIdMap(graph)
                .withTerminationFlag(computationResult.algorithm().terminationFlag)
                .withProgressTracker(progressTracker)
                .withArrowConnectionInfo(computationResult.config().arrowConnectionInfo())
                .parallel(Pools.DEFAULT, computationResult.config().writeConcurrency())
                .build();

            try {
                exporter.write(nodePropertyListFunction.apply(computationResult));
            } finally {
                progressTracker.release();
            }

            resultBuilder.withNodeCount(computationResult.graph().nodeCount());
            resultBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
        }
    }

    ProgressTracker createProgressTracker(
        long taskVolume,
        int writeConcurrency,
        ExecutionContext executionContext
    ) {
        return new TaskProgressTracker(
            NodePropertyExporter.baseTask(this.procedureName, taskVolume),
            executionContext.log(),
            writeConcurrency,
            executionContext.taskRegistryFactory()
        );
    }
}
