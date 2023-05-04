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
package org.neo4j.gds.beta.pregel.cc;

import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.pregel.proc.PregelMutateProc;
import org.neo4j.gds.pregel.proc.PregelMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@GdsCallable(
    name = "gds.pregel.test.mutate",
    executionMode = ExecutionMode.MUTATE_NODE_PROPERTY,
    description = "Test computation description"
)
@Generated("org.neo4j.gds.beta.pregel.PregelProcessor")
public final class ComputationMutateProc extends PregelMutateProc<ComputationAlgorithm, PregelProcedureConfig> {
    @Procedure(
        name = "gds.pregel.test.mutate",
        mode = Mode.READ
    )
    @Description("Test computation description")
    public Stream<PregelMutateResult> mutate(@Name("graphName") String graphName,
                                             @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return mutate(compute(graphName, configuration));
    }

    @Procedure(
        name = "gds.pregel.test.mutate.estimate",
        mode = Mode.READ
    )
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name("graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name("algoConfiguration") Map<String, Object> algoConfiguration) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected AbstractResultBuilder<PregelMutateResult> resultBuilder(
        ComputationResult<ComputationAlgorithm, PregelResult, PregelProcedureConfig> computeResult,
        ExecutionContext executionContext) {
        var ranIterations = computeResult.result().map(PregelResult::ranIterations).orElse(0);
        var didConverge = computeResult.result().map(PregelResult::didConverge).orElse(false);
        return new PregelMutateResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
    }

    @Override
    protected PregelProcedureConfig newConfig(String username, CypherMapWrapper config) {
        return PregelProcedureConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ComputationAlgorithm, PregelProcedureConfig> algorithmFactory(ExecutionContext executionContext) {
        return new GraphAlgorithmFactory<ComputationAlgorithm, PregelProcedureConfig>() {
            @Override
            public ComputationAlgorithm build(Graph graph, PregelProcedureConfig configuration,
                                              ProgressTracker progressTracker) {
                return new ComputationAlgorithm(graph, configuration, progressTracker);
            }

            @Override
            public String taskName() {
                return ComputationAlgorithm.class.getSimpleName();
            }

            @Override
            public Task progressTask(Graph graph, PregelProcedureConfig configuration) {
                return Pregel.progressTask(graph, configuration);
            }

            @Override
            public MemoryEstimation memoryEstimation(PregelProcedureConfig configuration) {
                var computation = new Computation();
                return Pregel.memoryEstimation(
                    computation.schema(configuration),
                    computation.reducer().isEmpty(),
                    configuration.isAsynchronous()
                );
            }
        };
    }
}
