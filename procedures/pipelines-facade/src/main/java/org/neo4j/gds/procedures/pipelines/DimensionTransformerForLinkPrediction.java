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

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;

import static org.neo4j.gds.ml.pipeline.PipelineCompanion.ANONYMOUS_GRAPH;

/**
 * Encapsulating a bit of variation
 */
class DimensionTransformerForLinkPrediction implements DimensionTransformer {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final DatabaseId databaseId;
    private final TrainedLPPipelineModel trainedLPPipelineModel;
    private final LinkPredictionPredictPipelineBaseConfig configuration;

    DimensionTransformerForLinkPrediction(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        DatabaseId databaseId,
        TrainedLPPipelineModel trainedLPPipelineModel,
        LinkPredictionPredictPipelineBaseConfig configuration
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.databaseId = databaseId;
        this.trainedLPPipelineModel = trainedLPPipelineModel;
        this.configuration = configuration;
    }

    @Override
    public GraphDimensions transform(GraphDimensions graphDimensions) {
        //Don't have nodeLabel information for filtering to give better estimation
        if (configuration.graphName().equals(ANONYMOUS_GRAPH)) return graphDimensions;

        var model = trainedLPPipelineModel.get(configuration.modelName(), configuration.username());

        var graphStore = graphStoreCatalogService.get(
            CatalogRequest.of(configuration.username(), databaseId),
            GraphName.parse(configuration.graphName())
        ).graphStore();

        var lpNodeLabelFilter = LPGraphStoreFilterFactory.generate(
            log,
            model.trainConfig(),
            configuration,
            graphStore
        );

        //Taking nodePropertyStepsLabels since they are superset of source&target nodeLabels, to give the upper bound estimation
        //In the future we can add nodeCount per label info to GraphDimensions to make more exact estimations
        return GraphDimensions.builder()
            .from(graphDimensions)
            .nodeCount(graphStore.getGraph(lpNodeLabelFilter.nodePropertyStepsBaseLabels()).nodeCount())
            .build();
    }
}
