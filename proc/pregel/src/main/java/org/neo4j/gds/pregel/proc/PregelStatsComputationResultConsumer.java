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
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

public class PregelStatsComputationResultConsumer<
    ALGO extends Algorithm<PregelResult>,
    CONFIG extends PregelProcedureConfig
  > implements ComputationResultConsumer<ALGO, PregelResult, CONFIG, Stream<PregelStatsResult>> {

    @Override
    public Stream<PregelStatsResult> consume(
        ComputationResult<ALGO, PregelResult, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        return runWithExceptionLogging("Stats call failed", executionContext.log(), () -> Stream.of(
            resultBuilder(computationResult, executionContext)
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(computationResult.config())
                .build()
        ));
    }

    protected AbstractPregelResultBuilder<PregelStatsResult> resultBuilder(
        ComputationResult<ALGO, PregelResult, CONFIG> computeResult,
        ExecutionContext executionContext
    ) {
        var ranIterations = computeResult.result().map(PregelResult::ranIterations).orElse(0);
        var didConverge = computeResult.result().map(PregelResult::didConverge).orElse(false);
        return new PregelStatsResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
    }
}
