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

import org.neo4j.gds.applications.algorithms.machinery.NodePropertyWriter;
import org.neo4j.gds.applications.algorithms.machinery.StandardLabel;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.WritePropertyConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public class WriteNodePropertiesComputationResultConsumer<ALGO extends Algorithm<ALGO_RESULT>, ALGO_RESULT, CONFIG extends WritePropertyConfig & AlgoBaseConfig, RESULT> implements ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, Stream<RESULT>> {
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
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult,
        ExecutionContext executionContext
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

    private void writeToNeo(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
            var log = executionContext.log();
            var nodePropertyExporterBuilder = executionContext.nodePropertyExporterBuilder();
            var taskRegistryFactory = executionContext.taskRegistryFactory();
            var terminationFlag = computationResult.algorithm().terminationFlag;
            var nodePropertyWriter = new NodePropertyWriter(
                log,
                nodePropertyExporterBuilder,
                taskRegistryFactory,
                terminationFlag
            );

            var graph = computationResult.graph();
            var graphStore = computationResult.graphStore();
            var config = computationResult.config();
            var resultStore = config.resolveResultStore(computationResult.resultStore());
            var nodeProperties = nodePropertyListFunction.apply(computationResult);

            var nodePropertiesWritten = nodePropertyWriter.writeNodeProperties(
                graph,
                graphStore,
                resultStore,
                nodeProperties,
                config.jobId(),
                new StandardLabel(procedureName),
                config
            );

            resultBuilder.withNodeCount(computationResult.graph().nodeCount());
            resultBuilder.withNodePropertiesWritten(nodePropertiesWritten.value());
        }
    }
}
