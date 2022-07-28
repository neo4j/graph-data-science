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

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.models.ClassifierFactory;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.getTrainedLPPipelineModel;
import static org.neo4j.gds.ml.pipeline.PipelineCompanion.ANONYMOUS_GRAPH;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LinkPredictionPredictPipelineAlgorithmFactory<CONFIG extends LinkPredictionPredictPipelineBaseConfig> extends GraphStoreAlgorithmFactory<LinkPredictionPredictPipelineExecutor, CONFIG> {
    private final ExecutionContext executionContext;
    private final ModelCatalog modelCatalog;

    LinkPredictionPredictPipelineAlgorithmFactory(ExecutionContext executionContext, ModelCatalog modelCatalog) {
        super();
        this.executionContext = executionContext;
        this.modelCatalog = modelCatalog;
    }

    @Override
    public Task progressTask(GraphStore graphStore, CONFIG config) {
        var pipeline = getTrainedLPPipelineModel(modelCatalog, config.modelName(), config.username())
            .customInfo()
            .pipeline();

        return LinkPredictionPredictPipelineExecutor.progressTask(taskName(), pipeline, graphStore, config);
    }

    @Override
    public String taskName() {
        return "Link Prediction Predict Pipeline";
    }

    @Override
    public LinkPredictionPredictPipelineExecutor build(
        GraphStore graphStore,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        var model = getTrainedLPPipelineModel(
            modelCatalog,
            configuration.modelName(),
            configuration.username()
        );

        var trainConfig = model.trainConfig();
        LPNodeLabelFilter lpNodeLabelFilter = generateNodeLabels(trainConfig, configuration, graphStore);


        progressTracker.logInfo(formatWithLocale(
            "The node labels used for filtering in prediction is: {sourceNodeLabel: %s, targetNodeLabel: %s, nodePropertyStepsLabels: %s}",
            lpNodeLabelFilter.internalSourceNodeLabels(),
            lpNodeLabelFilter.internalTargetNodeLabels(),
            lpNodeLabelFilter.nodePropertyStepsLabels()
        ));

        return new LinkPredictionPredictPipelineExecutor(
            model.customInfo().pipeline(),
            ClassifierFactory.create(model.data()),
            lpNodeLabelFilter,
            configuration,
            executionContext,
            graphStore,
            configuration.graphName(),
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        var model = getTrainedLPPipelineModel(
            modelCatalog,
            configuration.modelName(),
            configuration.username()
        );
        var linkPredictionPipeline = model.customInfo().pipeline();

        return LinkPredictionPredictPipelineExecutor.estimate(
            modelCatalog,
            linkPredictionPipeline,
            configuration,
            model.data()
        );
    }

    static Collection<NodeLabel> internalNodeLabels(GraphStore graphStore, String nodeLabel) {
        return (nodeLabel.equals(ElementProjection.PROJECT_ALL)) ? graphStore.nodeLabels() : List.of(NodeLabel.of(
            nodeLabel));
    }

    static Collection<NodeLabel> featureInputLabels(
        GraphStore graphStore,
        String sourceNodeLabel,
        String targetNodeLabel,
        List<String> contextNodeLabels
    ) {

        return (contextNodeLabels.contains(ElementProjection.PROJECT_ALL)
                || sourceNodeLabel.equals(ElementProjection.PROJECT_ALL) || targetNodeLabel.equals(ElementProjection.PROJECT_ALL))
            ? graphStore.nodeLabels()
            : Stream.concat(contextNodeLabels.stream(), Stream.of(sourceNodeLabel, targetNodeLabel))
                .map(NodeLabel::of)
                .collect(Collectors.toSet());
    }

    static LPNodeLabelFilter generateNodeLabels(
        LinkPredictionTrainConfig trainConfig,
        LinkPredictionPredictPipelineBaseConfig predictConfig,
        GraphStore graphStore
    ) {
        String sourceNodeLabel = predictConfig.sourceNodeLabel().orElse(trainConfig.sourceNodeLabel());
        String targetNodeLabel = predictConfig.targetNodeLabel().orElse(trainConfig.targetNodeLabel());
        List<String> contextNodeLabels;
        if (!predictConfig.contextNodeLabels().isEmpty()) {
            contextNodeLabels = predictConfig.contextNodeLabels();
        } else {
            contextNodeLabels = trainConfig.contextNodeLabels();
        }

        return ImmutableLPNodeLabelFilter.of(
            internalNodeLabels(graphStore, sourceNodeLabel),
            internalNodeLabels(graphStore, targetNodeLabel),
            featureInputLabels(graphStore, sourceNodeLabel, targetNodeLabel, contextNodeLabels)
        );

    }

    @Override
    public GraphDimensions estimatedGraphDimensionTransformer(GraphDimensions graphDimensions, CONFIG config) {
        var model = getTrainedLPPipelineModel(
            modelCatalog,
            config.modelName(),
            config.username()
        );

        //Don't have nodeLabel information for filtering to give better estimation
        if (config.graphName().equals(ANONYMOUS_GRAPH)) return graphDimensions;

        GraphStore graphStore = GraphStoreCatalog
            .get(CatalogRequest.of(config.username(), executionContext.databaseId().name()), config.graphName())
            .graphStore();

        LPNodeLabelFilter lpNodeLabelFilter = generateNodeLabels(model.trainConfig(), config, graphStore);

        //Taking nodePropertyStepsLabels since they are superset of source&target nodeLabels, to give the upper bound estimation
        //In the future we can add nodeCount per label info to GraphDimensions to make more exact estimations
        return GraphDimensions
            .builder()
            .from(graphDimensions)
            .nodeCount(graphStore.getGraph(lpNodeLabelFilter.nodePropertyStepsLabels()).nodeCount())
            .build();
    }

}
