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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.pipeline.predict.ApproximateLinkPrediction;
import org.neo4j.gds.ml.linkmodels.pipeline.predict.ExhaustiveLinkPrediction;
import org.neo4j.gds.ml.linkmodels.pipeline.predict.LinkPrediction;
import org.neo4j.gds.ml.linkmodels.pipeline.predict.LinkPredictionPipelineBaseConfig;
import org.neo4j.gds.ml.pipeline.ImmutableGraphFilter;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;

import java.util.Map;

public class LinkPredictionPipelinePredictExecutor extends PipelineExecutor<
    LinkFeatureStep,
    double[],
    LinkLogisticRegressionTrainConfig,
    LinkPredictionResult,
    LinkPredictionPipelinePredictExecutor
    > {

    private final LinkLogisticRegressionData linkLogisticRegressionData;
    private final Graph graph;
    private final LinkPredictionPipelineBaseConfig lpConfig; // TODO fix this

    public LinkPredictionPipelinePredictExecutor(
        LinkPredictionPipeline pipeline,
        LinkLogisticRegressionData linkLogisticRegressionData,
        LinkPredictionPipelineBaseConfig config,
        BaseProc caller,
        GraphStore graphStore,
        Graph graph,
        String graphName,
        ProgressTracker progressTracker
    ) {
        super(pipeline, config, caller, graphStore, graphName, progressTracker);
        this.linkLogisticRegressionData = linkLogisticRegressionData;
        this.graph = graph;
        this.lpConfig = config;
    }

    @Override
    public Map<DatasetSplits, GraphFilter> splitDataset() {
        return Map.of(
            DatasetSplits.TEST,
            ImmutableGraphFilter.of(
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            ),
            DatasetSplits.TRAIN,
            ImmutableGraphFilter.of(
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            ),
            DatasetSplits.FEATURE_INPUT,
            ImmutableGraphFilter.of(
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            ),
            DatasetSplits.TEST_COMPLEMENT,
            ImmutableGraphFilter.of(
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            )
        );
    }

    @Override
    public LinkPredictionPipelinePredictExecutor me() {
        return this;
    }

    @Override
    protected LinkPredictionResult train(Map<DatasetSplits, GraphFilter> dataSplits) {
        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, pipeline.featureSteps());
        var linkPrediction = getLinkPredictionStrategy(lpConfig.isApproximateStrategy(), linkFeatureExtractor);
        return linkPrediction.compute();
    }

    private LinkPrediction getLinkPredictionStrategy(
        boolean isApproximateStrategy,
        LinkFeatureExtractor linkFeatureExtractor
    ) {
        if (isApproximateStrategy) {
            return new ApproximateLinkPrediction(
                linkLogisticRegressionData,
                linkFeatureExtractor,
                graph,
                lpConfig.approximateConfig(),
                progressTracker
            );
        } else {
            return new ExhaustiveLinkPrediction(
                linkLogisticRegressionData,
                linkFeatureExtractor,
                graph,
                lpConfig.concurrency(),
                lpConfig.topN().orElseThrow(),
                lpConfig.thresholdOrDefault(),
                progressTracker
            );
        }
    }
}
