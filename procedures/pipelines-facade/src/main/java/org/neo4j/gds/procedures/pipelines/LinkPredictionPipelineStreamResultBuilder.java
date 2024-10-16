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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;

import java.util.Optional;
import java.util.stream.Stream;

class LinkPredictionPipelineStreamResultBuilder implements StreamResultBuilder<LinkPredictionResult, StreamResult> {
    private final Log log;
    private final TrainedLPPipelineModel trainedLPPipelineModel;
    private final LinkPredictionPredictPipelineStreamConfig configuration;

    public LinkPredictionPipelineStreamResultBuilder(
        Log log,
        TrainedLPPipelineModel trainedLPPipelineModel,
        LinkPredictionPredictPipelineStreamConfig configuration
    ) {
        this.log = log;
        this.trainedLPPipelineModel = trainedLPPipelineModel;
        this.configuration = configuration;
    }

    @Override
    public Stream<StreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<LinkPredictionResult> result
    ) {
        if (result.isEmpty()) return Stream.empty();

        var linkPredictionResult = result.get();

        var model = trainedLPPipelineModel.get(
            configuration.modelName(),
            configuration.username()
        );

        var lpGraphStoreFilter = LPGraphStoreFilterFactory.generate(
            log, model.trainConfig(),
            configuration,
            graphStore
        );

        var filteredGraph = graphStore.getGraph(lpGraphStoreFilter.predictNodeLabels());

        return linkPredictionResult.stream().map(predictedLink -> new StreamResult(
            filteredGraph.toOriginalNodeId(predictedLink.sourceId()),
            filteredGraph.toOriginalNodeId(predictedLink.targetId()),
            predictedLink.probability()
        ));
    }
}
