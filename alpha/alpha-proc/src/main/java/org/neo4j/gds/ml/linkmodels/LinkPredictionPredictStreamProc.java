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
package org.neo4j.gds.ml.linkmodels;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.ml.linkmodels.LinkPredictionPredictCompanion.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.ml.linkPrediction.predict.stream", description = DESCRIPTION, executionMode = STREAM)
public class LinkPredictionPredictStreamProc extends AlgoBaseProc<LinkPredictionPredict, ExhaustiveLinkPredictionResult, LinkPredictionPredictStreamConfig, LinkPredictionPredictStreamProc.Result> {

    @Procedure(name = "gds.alpha.ml.linkPrediction.predict.stream", mode = Mode.READ)
    @Description(DESCRIPTION)
    public Stream<Result> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(graphName, configuration);
        return computationResultConsumer().consume(result, executionContext());
    }

    @Procedure(name = "gds.alpha.ml.linkPrediction.predict.stream.estimate", mode = READ)
    @Description("Estimates memory for applying a linkPrediction model")
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    public ValidationConfiguration<LinkPredictionPredictStreamConfig> validationConfig() {
        return LinkPredictionPredictCompanion.getValidationConfig();
    }

    @Override
    public AlgorithmSpec<LinkPredictionPredict, ExhaustiveLinkPredictionResult, LinkPredictionPredictStreamConfig, Stream<Result>, AlgorithmFactory<?, LinkPredictionPredict, LinkPredictionPredictStreamConfig>> withModelCatalog(
        ModelCatalog modelCatalog
    ) {
        this.setModelCatalog(modelCatalog);
        return this;
    }

    @Override
    protected LinkPredictionPredictStreamConfig newConfig(String username, CypherMapWrapper config) {
        return LinkPredictionPredictStreamConfig.of(username, config);
    }

    @Override
    public GraphAlgorithmFactory<LinkPredictionPredict, LinkPredictionPredictStreamConfig> algorithmFactory() {
        return new LinkPredictionPredictFactory<>(modelCatalog());
    }

    @Override
    public ComputationResultConsumer<LinkPredictionPredict, ExhaustiveLinkPredictionResult, LinkPredictionPredictStreamConfig, Stream<Result>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();

            if (computationResult.isGraphEmpty()) {
                graph.release();
                return Stream.empty();
            }

            return computationResult.result().stream()
                .map(predictedLink -> new Result(
                    graph.toOriginalNodeId(predictedLink.sourceId()),
                    graph.toOriginalNodeId(predictedLink.targetId()),
                    predictedLink.probability()
                ));
        };
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
