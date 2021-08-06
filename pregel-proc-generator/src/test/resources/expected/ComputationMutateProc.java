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
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelMutateProc;
import org.neo4j.gds.beta.pregel.PregelMutateResult;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Generated("org.neo4j.gds.beta.pregel.PregelProcessor")
public final class ComputationMutateProc extends PregelMutateProc<ComputationAlgorithm, PregelProcedureConfig> {
    @Procedure(
            name = "gds.pregel.test.mutate",
            mode = Mode.READ
    )
    @Description("Test computation description")
    public Stream<PregelMutateResult> mutate(@Name("graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(
            name = "gds.pregel.test.mutate.estimate",
            mode = Mode.READ
    )
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> mutateEstimate(@Name("graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected AbstractResultBuilder<PregelMutateResult> resultBuilder(
            AlgoBaseProc.ComputationResult<ComputationAlgorithm, PregelResult, PregelProcedureConfig> computeResult) {
        var ranIterations = computeResult.result().ranIterations();
        var didConverge = computeResult.result().didConverge();
        return new PregelMutateResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
    }

    @Override
    protected PregelProcedureConfig newConfig(String username, Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate, CypherMapWrapper config) {
        return PregelProcedureConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ComputationAlgorithm, PregelProcedureConfig> algorithmFactory() {
        return new AbstractAlgorithmFactory<ComputationAlgorithm, PregelProcedureConfig>() {
            @Override
            public ComputationAlgorithm build(Graph graph, PregelProcedureConfig configuration,
                                              AllocationTracker tracker, ProgressTracker progressTracker) {
                return new ComputationAlgorithm(graph, configuration, tracker, progressTracker.progressLogger());
            }

            @Override
            protected String taskName() {
                return ComputationAlgorithm.class.getSimpleName();
            }

            @Override
            public MemoryEstimation memoryEstimation(PregelProcedureConfig configuration) {
                var computation = new Computation();
                return Pregel.memoryEstimation(computation.schema(configuration), computation.reducer().isPresent(), configuration.isAsynchronous());
            }
        };
    }
}
