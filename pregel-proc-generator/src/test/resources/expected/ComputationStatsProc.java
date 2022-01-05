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
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.pregel.proc.PregelStatsProc;
import org.neo4j.gds.pregel.proc.PregelStatsResult;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@GdsCallable(
    name = "gds.pregel.test.stats",
    executionMode = ExecutionMode.STATS,
    description = "Test computation description"
)
@Generated("org.neo4j.gds.beta.pregel.PregelProcessor")
public final class ComputationStatsProc extends PregelStatsProc<ComputationAlgorithm, PregelProcedureConfig> {
    @Procedure(
        name = "gds.pregel.test.stats",
        mode = Mode.READ
    )
    @Description("Test computation description")
    public Stream<PregelStatsResult> stats(@Name("graphName") String graphName,
                                           @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return stats(compute(graphName, configuration));
    }

    @Procedure(
        name = "gds.pregel.test.stats.estimate",
        mode = Mode.READ
    )
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name("graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name("algoConfiguration") Map<String, Object> algoConfiguration) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected AbstractResultBuilder<PregelStatsResult> resultBuilder(
        ComputationResult<ComputationAlgorithm, PregelResult, PregelProcedureConfig> computeResult,
        ExecutionContext executionContext) {
        var ranIterations = computeResult.result().ranIterations();
        var didConverge = computeResult.result().didConverge();
        return new PregelStatsResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
    }

    @Override
    protected PregelProcedureConfig newConfig(String username, CypherMapWrapper config) {
        return PregelProcedureConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ComputationAlgorithm, PregelProcedureConfig> algorithmFactory() {
        return new GraphAlgorithmFactory<ComputationAlgorithm, PregelProcedureConfig>() {
            @Override
            public ComputationAlgorithm build(Graph graph, PregelProcedureConfig configuration,
                                              AllocationTracker allocationTracker, ProgressTracker progressTracker) {
                return new ComputationAlgorithm(graph, configuration, allocationTracker, progressTracker);
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
                return Pregel.memoryEstimation(computation.schema(configuration), computation.reducer().isPresent(), configuration.isAsynchronous());
            }
        };
    }
}