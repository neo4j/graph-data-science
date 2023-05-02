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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.ESTIMATE_PREDICT_DESCRIPTION;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.PREDICT_DESCRIPTION;
import static org.neo4j.gds.ml.pipeline.PipelineCompanion.preparePipelineConfig;

@GdsCallable(name = "gds.beta.pipeline.linkPrediction.predict.stream", description = PREDICT_DESCRIPTION, executionMode = STREAM)
public class LinkPredictionPipelineStreamProc extends AlgoBaseProc<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPredictPipelineStreamConfig, LinkPredictionPipelineStreamProc.Result> {

    @Procedure(name = "gds.beta.pipeline.linkPrediction.predict.stream", mode = Mode.READ)
    @Description(PREDICT_DESCRIPTION)
    public Stream<Result> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        preparePipelineConfig(graphName, configuration);
        var result = compute(graphName, configuration);
        return computationResultConsumer().consume(result, executionContext());
    }

    @Procedure(name = "gds.beta.pipeline.linkPrediction.predict.stream.estimate", mode = Mode.READ)
    @Description(ESTIMATE_PREDICT_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        preparePipelineConfig(graphNameOrConfiguration, algoConfiguration);
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LinkPredictionPredictPipelineStreamConfig newConfig(String username, CypherMapWrapper config) {
        return LinkPredictionPredictPipelineStreamConfig.of(username, config);
    }

    @Override
    public GraphStoreAlgorithmFactory<LinkPredictionPredictPipelineExecutor, LinkPredictionPredictPipelineStreamConfig> algorithmFactory() {
        return new LinkPredictionPredictPipelineAlgorithmFactory<>(executionContext());
    }

    @Override
    public ComputationResultConsumer<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPredictPipelineStreamConfig, Stream<Result>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.result().isEmpty()) {
                return Stream.empty();
            }

            var graphStore = computationResult.graphStore();
            Collection<NodeLabel> labelFilter = computationResult.algorithm().labelFilter().predictNodeLabels();
            var graph = graphStore.getGraph(labelFilter);

            return computationResult.result().get().stream()
                .map(predictedLink -> new Result(
                    graph.toOriginalNodeId(predictedLink.sourceId()),
                    graph.toOriginalNodeId(predictedLink.targetId()),
                    predictedLink.probability()
                ));
        };
    }

    @Override
    public AlgorithmSpec<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPredictPipelineStreamConfig, Stream<Result>, AlgorithmFactory<?, LinkPredictionPredictPipelineExecutor, LinkPredictionPredictPipelineStreamConfig>> withModelCatalog(
        ModelCatalog modelCatalog
    ) {
        this.setModelCatalog(modelCatalog);
        return this;
    }

    @SuppressWarnings("unused")
    public static final class Result {

        public final long node1;
        public final long node2;
        public final double probability;

        public Result(long node1, long node2, double probability) {
            this.node1 = node1;
            this.node2 = node2;
            this.probability = probability;
        }
    }
}
