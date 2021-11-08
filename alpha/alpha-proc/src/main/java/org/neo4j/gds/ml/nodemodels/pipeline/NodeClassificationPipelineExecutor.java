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

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrain;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainCoreConfig;
import org.neo4j.gds.ml.pipeline.ImmutableGraphFilter;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class NodeClassificationPipelineExecutor extends PipelineExecutor<
    NodeClassificationPipelineTrainConfig,
    NodeClassificationPipeline,
    Model<
        NodeLogisticRegressionData,
        NodeClassificationPipelineTrainConfig,
        NodeClassificationPipelineModelInfo>,
    NodeClassificationPipelineExecutor>
{
    public static final String MODEL_TYPE = "Node classification pipeline";

    public NodeClassificationPipelineExecutor(
        NodeClassificationPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        BaseProc caller,
        GraphStore graphStore,
        String graphName,
        ProgressTracker progressTracker
    ) {
        super(pipeline, config, caller, graphStore, graphName, progressTracker);
    }

    @Override
    public Map<DatasetSplits, GraphFilter> splitDataset() {
        return Map.of(
            DatasetSplits.FEATURE_INPUT,
            ImmutableGraphFilter.of(
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            )
        );
    }

    @Override
    protected Model<
        NodeLogisticRegressionData,
        NodeClassificationPipelineTrainConfig,
        NodeClassificationPipelineModelInfo>
    execute(Map<DatasetSplits, GraphFilter> dataSplits) {
        var nodeLabels = config.nodeLabelIdentifiers(graphStore);
        var relationshipTypes = config.internalRelationshipTypes(graphStore);
        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, Optional.empty());
        var innerModel = NodeClassificationTrain
            .create(graph, innerConfig(), caller.allocationTracker, progressTracker)
            .compute();

        var innerInfo = innerModel.customInfo();

        var modelInfo = NodeClassificationPipelineModelInfo.builder()
            .classes(innerInfo.classes())
            .bestParameters(innerInfo.bestParameters())
            .metrics(innerInfo.metrics())
            .trainingPipeline(pipeline.copy())
            .build();

        return Model.of(
            innerModel.creator(),
            innerModel.name(),
            MODEL_TYPE,
            innerModel.graphSchema(),
            innerModel.data(),
            config,
            modelInfo
        );
    }

    @Override
    public NodeClassificationPipelineExecutor me() {
        return this;
    }

    public NodeClassificationTrainConfig innerConfig() {
        var params = pipeline.trainingParameterSpace().stream()
            .map(NodeLogisticRegressionTrainCoreConfig::toMap).collect(Collectors.toList());
        return NodeClassificationTrainConfig.builder()
            .modelName(config.modelName())
            .concurrency(config.concurrency())
            .metrics(config.metrics())
            .targetProperty(config.targetProperty())
            .featureProperties(pipeline.featureProperties())
            .params(params)
            .holdoutFraction(pipeline.splitConfig().holdoutFraction())
            .validationFolds(pipeline.splitConfig().validationFolds())
            .build();
    }
}
