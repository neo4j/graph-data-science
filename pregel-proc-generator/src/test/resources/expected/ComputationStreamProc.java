package org.neo4j.graphalgo.beta.pregel.cc;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.StreamProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.beta.pregel.PregelStreamResult;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Generated("org.neo4j.graphalgo.beta.pregel.PregelProcessor")
public final class ComputationStreamProc extends StreamProc<ComputationAlgorithm, Pregel.PregelResult, PregelStreamResult, PregelConfig> {
    @Procedure(
            name = "gds.pregel.test.stream",
            mode = Mode.READ
    )
    @Description("Test computation description")
    public Stream<PregelStreamResult> stream(@Name("graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return stream(compute(graphNameOrConfig, configuration));
    }

    @Procedure(
            name = "gds.pregel.test.stream.estimate",
            mode = Mode.READ
    )
    @Description(BaseProc.ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> streamEstimate(@Name("graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected PregelStreamResult streamResult(long originalNodeId, long internalNodeId,
            NodeProperties nodeProperties) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected PregelConfig newConfig(String username, Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate, CypherMapWrapper config) {
        return PregelConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ComputationAlgorithm, PregelConfig> algorithmFactory() {
        return new AlgorithmFactory<ComputationAlgorithm, PregelConfig>() {
            @Override
            public ComputationAlgorithm build(Graph graph, PregelConfig configuration,
                    AllocationTracker tracker, Log log) {
                return new ComputationAlgorithm(graph, configuration, tracker, log);
            }

            @Override
            public MemoryEstimation memoryEstimation(PregelConfig configuration) {
                return Pregel.memoryEstimation();
            }
        };
    }

    @Override
    protected NodeProperties getNodeProperties(
            AlgoBaseProc.ComputationResult<ComputationAlgorithm, Pregel.PregelResult, PregelConfig> computationResult) {
        return (DoubleNodeProperties) computationResult.result().nodeValues()::get;
    }

    @Override
    protected Stream<PregelStreamResult> stream(
            AlgoBaseProc.ComputationResult<ComputationAlgorithm, Pregel.PregelResult, PregelConfig> computationResult) {
        if (computationResult.isGraphEmpty()) {
            return Stream.empty();
        }
        var result = computationResult.result().compositeNodeValues();
        return LongStream.range(IdMapping.START_NODE_ID, computationResult.graph().nodeCount()).mapToObj(nodeId -> {
            Map<String, Object> values = result.schema().elements().stream().collect(Collectors.toMap(
            Pregel.Element::propertyKey,
            element -> {
                if (element.propertyType() == ValueType.DOUBLE) {
                    return result.doubleProperties(element.propertyKey()).get(nodeId);
                }
                return result.longProperties(element.propertyKey()).get(nodeId);
            }
            ));
            return new PregelStreamResult(nodeId, values);
        } );

    }
}
