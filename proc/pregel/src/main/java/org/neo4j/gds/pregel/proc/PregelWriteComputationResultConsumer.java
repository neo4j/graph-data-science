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
package org.neo4j.gds.pregel.proc;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.StandardLabel;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public class PregelWriteComputationResultConsumer<
    ALGO extends Algorithm<PregelResult>,
    CONFIG extends PregelProcedureConfig
  > implements
    ComputationResultConsumer<ALGO, PregelResult, CONFIG, Stream<PregelWriteResult>> {

    @Override
    public Stream<PregelWriteResult> consume(
        ComputationResult<ALGO, PregelResult, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        return runWithExceptionLogging(
            "Graph write failed", executionContext.log(), () -> {
                CONFIG config = computationResult.config();
                var ranIterations = computationResult.result().map(PregelResult::ranIterations).orElse(0);
                var didConverge = computationResult.result().map(PregelResult::didConverge).orElse(false);

                AtomicLong writeMillis = new AtomicLong();
                AtomicLong nodePropertiesWritten = new AtomicLong();
                try (ProgressTimer ignored = ProgressTimer.start(writeMillis::set)) {
                    if (!computationResult.isGraphEmpty()) {
                        var nodePropertyList = PregelCompanion.nodeProperties(
                            computationResult,
                            config.writeProperty()
                        );

                        var writeContext = WriteContext
                            .builder()
                            .with(executionContext.nodePropertyExporterBuilder())
                            .build();

                        var requestScopedDependencies = RequestScopedDependencies
                            .builder()
                            .terminationFlag(computationResult.algorithm().getTerminationFlag())
                            .taskRegistryFactory(executionContext.taskRegistryFactory())
                            .build();

                        var log  = executionContext.log();

                        var writeToDatabase = new WriteNodePropertyService(
                            log,
                            requestScopedDependencies,
                            writeContext
                        );

                        var resultStore = config.resolveResultStore(computationResult.resultStore()).orElse(ResultStore.EMPTY);

                        var nodePropsWritten = writeToDatabase.perform(
                            computationResult.graph(),
                            computationResult.graphStore(),
                            resultStore,
                            config,
                            new StandardLabel("PregelWrite"),
                            config.jobId(),
                            nodePropertyList
                            );

                        nodePropertiesWritten.set(nodePropsWritten.value());
                    }
                }

                var result = PregelWriteResult.create(
                    nodePropertiesWritten.get(),
                    computationResult.preProcessingMillis(),
                    computationResult.computeMillis(),
                    writeMillis.get(),
                    ranIterations,
                    didConverge,
                    config.toMap()
                );

                return Stream.of(result);
            }
        );
    }
}


