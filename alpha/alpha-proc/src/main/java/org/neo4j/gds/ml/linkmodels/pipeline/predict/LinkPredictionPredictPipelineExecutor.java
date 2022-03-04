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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.pipeline.ImmutableGraphFilter;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPipeline;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LinkPredictionPredictPipelineExecutor extends PipelineExecutor<
    LinkPredictionPredictPipelineBaseConfig,
    LinkPredictionPipeline,
    LinkPredictionResult
    > {
    private final LogisticRegressionData logisticRegressionData;

    public LinkPredictionPredictPipelineExecutor(
        LinkPredictionPipeline pipeline,
        LogisticRegressionData logisticRegressionData,
        LinkPredictionPredictPipelineBaseConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        String graphName,
        ProgressTracker progressTracker
    ) {
        super(pipeline, config, executionContext, graphStore, graphName, progressTracker);
        this.logisticRegressionData = logisticRegressionData;
    }

    @Override
    public Map<DatasetSplits, GraphFilter> splitDataset() {
        // For prediction, we don't split the input graph but generate the features and predict over the whole graph
        return Map.of(
            DatasetSplits.FEATURE_INPUT,
            ImmutableGraphFilter.of(
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            )
        );
    }

    @Override
    protected LinkPredictionResult execute(Map<DatasetSplits, GraphFilter> dataSplits) {
        var graph = graphStore.getGraph(
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore),
            Optional.empty()
        );

        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, pipeline.featureSteps());
        var linkPrediction = getLinkPredictionStrategy(graph, config.isApproximateStrategy(), linkFeatureExtractor);
        return linkPrediction.compute();
    }

    public static MemoryEstimation estimate(
        ModelCatalog modelCatalog,
        LinkPredictionPipeline pipeline,
        LinkPredictionPredictPipelineBaseConfig configuration,
        int linkFeatureDimension
    ) {
        MemoryEstimation maxOverNodePropertySteps = PipelineExecutor.estimateNodePropertySteps(
            modelCatalog,
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            configuration.relationshipTypes()
        );

        var predictEstimation = configuration.isApproximateStrategy()
            ? ApproximateLinkPrediction.estimate(configuration)
            : ExhaustiveLinkPrediction.estimate(configuration, linkFeatureDimension);

        return MemoryEstimations.builder(LinkPredictionPredictPipelineExecutor.class)
            .max("Pipeline execution", List.of(maxOverNodePropertySteps, predictEstimation))
            .build();
    }

    private LinkPrediction getLinkPredictionStrategy(
        Graph graph,
        boolean isApproximateStrategy,
        LinkFeatureExtractor linkFeatureExtractor
    ) {
        if (isApproximateStrategy) {
            return new ApproximateLinkPrediction(
                logisticRegressionData,
                linkFeatureExtractor,
                graph,
                config.approximateConfig(),
                progressTracker
            );
        } else {
            return new ExhaustiveLinkPrediction(
                logisticRegressionData,
                linkFeatureExtractor,
                graph,
                config.concurrency(),
                config.topN().orElseThrow(),
                config.thresholdOrDefault(),
                progressTracker
            );
        }
    }

}
