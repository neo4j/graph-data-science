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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;

import java.util.Collection;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.PREDICT_DESCRIPTION;

@GdsCallable(
    name = "gds.beta.pipeline.linkPrediction.predict.stream", description = PREDICT_DESCRIPTION, executionMode = STREAM
)
public class LinkPredictionPipelineStreamSpec implements
    AlgorithmSpec<
        LinkPredictionPredictPipelineExecutor,
        LinkPredictionResult,
        LinkPredictionPredictPipelineStreamConfig,
        Stream<StreamResult>,
        LinkPredictionPredictPipelineAlgorithmFactory<LinkPredictionPredictPipelineStreamConfig>> {
    @Override
    public String name() {
        return "LinkPredictionPipelineStream";
    }

    @Override
    public LinkPredictionPredictPipelineAlgorithmFactory<LinkPredictionPredictPipelineStreamConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new LinkPredictionPredictPipelineAlgorithmFactory<>(executionContext);
    }

    @Override
    public NewConfigFunction<LinkPredictionPredictPipelineStreamConfig> newConfigFunction() {
        return LinkPredictionPredictPipelineStreamConfig::of;
    }

    @Override
    public ComputationResultConsumer<LinkPredictionPredictPipelineExecutor, LinkPredictionResult, LinkPredictionPredictPipelineStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            return computationResult.result().map(result -> {
                var graphStore = computationResult.graphStore();
                Collection<NodeLabel> labelFilter = computationResult.algorithm().labelFilter().predictNodeLabels();
                var graph = graphStore.getGraph(labelFilter);

                return result.stream()
                    .map(predictedLink -> new StreamResult(
                        graph.toOriginalNodeId(predictedLink.sourceId()),
                        graph.toOriginalNodeId(predictedLink.targetId()),
                        predictedLink.probability()
                    ));
            }).orElseGet(Stream::empty);
        };
    }
}
