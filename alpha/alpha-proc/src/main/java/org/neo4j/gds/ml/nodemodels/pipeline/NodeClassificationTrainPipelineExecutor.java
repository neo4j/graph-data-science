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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.BestMetricData;
import org.neo4j.gds.ml.nodemodels.BestModelStats;
import org.neo4j.gds.ml.nodemodels.ImmutableModelSelectResult;
import org.neo4j.gds.ml.nodemodels.Metric;
import org.neo4j.gds.ml.nodemodels.MetricData;
import org.neo4j.gds.ml.nodemodels.ModelStats;
import org.neo4j.gds.ml.nodemodels.NodeClassificationModelInfo;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrain;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainConfigImpl;
import org.neo4j.gds.ml.pipeline.ImmutableGraphFilter;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineTrainConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class NodeClassificationTrainPipelineExecutor extends PipelineExecutor<
    NodeClassificationPipelineTrainConfig,
    NodeClassificationPipeline,
    NodeClassificationPipelineTrainResult
> {

    public NodeClassificationTrainPipelineExecutor(
        NodeClassificationPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        String graphName,
        ProgressTracker progressTracker
    ) {
        super(pipeline, config, executionContext, graphStore, graphName, progressTracker);
    }

    public static MemoryEstimation estimate(
        NodeClassificationPipeline pipeline,
        NodeClassificationPipelineTrainConfig configuration,
        ModelCatalog modelCatalog
    ) {
        MemoryEstimation nodePropertyStepsEstimation = PipelineExecutor.estimateNodePropertySteps(
            modelCatalog,
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            configuration.relationshipTypes()
        );

        var trainingEstimation = MemoryEstimations
            .builder()
            .add("Pipeline Train", NodeClassificationTrain.estimate(innerConfig(pipeline, configuration)))
            .build();

        return MemoryEstimations.maxEstimation(
            "Pipeline executor",
            List.of(nodePropertyStepsEstimation, trainingEstimation)
        );
    }

    @Override
    public Map<DatasetSplits, GraphFilter> splitDataset() {
        // we don't split the input graph but generate the features and predict over the whole graph.
        // Inside the training algo we split the nodes into multiple sets.
        return Map.of(
            DatasetSplits.FEATURE_INPUT,
            ImmutableGraphFilter.of(
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            )
        );
    }

    @Override
    protected NodeClassificationPipelineTrainResult execute(Map<DatasetSplits, GraphFilter> dataSplits) {
        var nodeLabels = config.nodeLabelIdentifiers(graphStore);
        var relationshipTypes = config.internalRelationshipTypes(graphStore);
        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, Optional.empty());

        this.pipeline.splitConfig().validateMinNumNodesInSplitSets(graph);

        var innerModel = NodeClassificationTrain
            .create(graph, innerConfig(pipeline, config), progressTracker)
            .compute();
        var innerInfo = innerModel.customInfo();

        var bestMetrics = innerInfo.metrics().entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            metricStats -> extractWinningModelStats(metricStats.getValue(), innerInfo.bestParameters())
        ));
        var modelInfo = NodeClassificationPipelineModelInfo.builder()
            .classes(innerInfo.classes())
            .bestParameters(innerInfo.bestParameters())
            .metrics(bestMetrics)
            .trainingPipeline(pipeline.copy())
            .build();

        return ImmutableNodeClassificationPipelineTrainResult.of(
            Model.of(
                innerModel.creator(),
                innerModel.name(),
                NodeClassificationPipeline.MODEL_TYPE,
                innerModel.graphSchema(),
                innerModel.data(),
                config,
                modelInfo
            ),
            ImmutableModelSelectResult.of(
                innerInfo.bestParameters(),
                getTrainingStats(innerInfo),
                getValidationStats(innerInfo)
            )
        );
    }


    private Map<Metric, List<ModelStats<LogisticRegressionTrainConfig>>> getTrainingStats(NodeClassificationModelInfo innerInfo) {
        return innerInfo.metrics().entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            metricStats -> metricStats.getValue().train())
        );
    }

    private Map<Metric, List<ModelStats<LogisticRegressionTrainConfig>>> getValidationStats(NodeClassificationModelInfo innerInfo) {
        return innerInfo.metrics().entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            metricStats -> metricStats.getValue().validation())
        );
    }

    private BestMetricData extractWinningModelStats(
        MetricData<LogisticRegressionTrainConfig> oldStats,
        LogisticRegressionTrainConfig bestParams
    ) {
        return BestMetricData.of(
            findBestModelStats(oldStats.train(), bestParams),
            findBestModelStats(oldStats.validation(), bestParams),
            oldStats.outerTrain(),
            oldStats.test()
        );

    }

    static NodeClassificationTrainConfig innerConfig(NodeClassificationPipeline pipeline, NodeClassificationPipelineTrainConfig config) {
        var params = pipeline.trainingParameterSpace().stream()
            .map(LogisticRegressionTrainConfig::toMap).collect(Collectors.toList());
        return NodeClassificationTrainConfigImpl.builder()
            .modelName(config.modelName())
            .concurrency(config.concurrency())
            .username(config.username())
            .metrics(config.metrics())
            .targetProperty(config.targetProperty())
            .featureProperties(pipeline.featureProperties())
            .params(params)
            .randomSeed(config.randomSeed())
            .holdoutFraction(pipeline.splitConfig().testFraction())
            .validationFolds(pipeline.splitConfig().validationFolds())
            .nodeLabels(config.nodeLabels())
            .relationshipTypes(config.relationshipTypes())
            .build();
    }

    private static BestModelStats findBestModelStats(
        List<ModelStats<LogisticRegressionTrainConfig>> metricStatsForModels,
        LogisticRegressionTrainConfig bestParams
    ) {
        return metricStatsForModels.stream()
            .filter(metricStatsForModel -> metricStatsForModel.params() == bestParams)
            .findFirst()
            .map(BestModelStats::of)
            .orElseThrow();
    }
}
