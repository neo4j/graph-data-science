package org.neo4j.gds.beta.pregel.cc;

import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.pregel.proc.PregelBaseProc;
import org.neo4j.gds.pregel.proc.PregelStreamProc;
import org.neo4j.gds.pregel.proc.PregelStreamResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@GdsCallable(
    name = "gds.pregel.bidirectionalTest.stream",
    executionMode = ExecutionMode.STREAM,
    description = "Bidirectional Test computation description"
)
@Generated("org.neo4j.gds.beta.pregel.PregelProcessor")
public final class BidirectionalComputationStreamProc extends PregelStreamProc<BidirectionalComputationAlgorithm, PregelProcedureConfig> {
    @Procedure(
        name = "gds.pregel.bidirectionalTest.stream",
        mode = Mode.READ
    )
    @Description("Bidirectional Test computation description")
    public Stream<PregelStreamResult> stream(@Name("graphName") String graphName,
                                             @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return stream(compute(graphName, configuration));
    }

    @Procedure(
        name = "gds.pregel.bidirectionalTest.stream.estimate",
        mode = Mode.READ
    )
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name("graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name("algoConfiguration") Map<String, Object> algoConfiguration) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected PregelStreamResult streamResult(long originalNodeId, long internalNodeId,
                                              NodePropertyValues nodePropertyValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected PregelProcedureConfig newConfig(String username, CypherMapWrapper config) {
        return PregelProcedureConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<BidirectionalComputationAlgorithm, PregelProcedureConfig> algorithmFactory(
    ) {
        return new GraphAlgorithmFactory<BidirectionalComputationAlgorithm, PregelProcedureConfig>() {
            @Override
            public BidirectionalComputationAlgorithm build(Graph graph,
                                                           PregelProcedureConfig configuration, ProgressTracker progressTracker) {
                return new BidirectionalComputationAlgorithm(graph, configuration, progressTracker);
            }

            @Override
            public String taskName() {
                return BidirectionalComputationAlgorithm.class.getSimpleName();
            }

            @Override
            public Task progressTask(Graph graph, PregelProcedureConfig configuration) {
                return Pregel.progressTask(graph, configuration);
            }

            @Override
            public MemoryEstimation memoryEstimation(PregelProcedureConfig configuration) {
                var computation = new BidirectionalComputation();
                return Pregel.memoryEstimation(computation.schema(configuration), computation.reducer().isEmpty(), configuration.isAsynchronous());
            }
        };
    }

    @Override
    public ValidationConfiguration<PregelProcedureConfig> validationConfig() {
        return PregelBaseProc.ensureIndexValidation(log, taskRegistryFactory);
    }
}
