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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.pipeline.ImmutableGraphFilter;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.training.TrainingStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationTrainPipelineExecutor.NodeClassificationTrainPipelineResult;

public class NodeClassificationTrainPipelineExecutor extends PipelineExecutor<
    NodeClassificationPipelineTrainConfig,
    NodeClassificationTrainingPipeline,
    NodeClassificationTrainPipelineResult
> {

    public NodeClassificationTrainPipelineExecutor(
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        String graphName,
        ProgressTracker progressTracker
    ) {
        super(pipeline, config, executionContext, graphStore, graphName, progressTracker);
    }

    public static Task progressTask(String taskName, NodeClassificationTrainingPipeline pipeline, long nodeCount) {
        return Tasks.task(
            taskName,
            new ArrayList<>() {{
                add(nodePropertyStepTasks(pipeline.nodePropertySteps(), nodeCount));
                addAll(NodeClassificationTrain.progressTasks(
                    pipeline.splitConfig(),
                    pipeline.numberOfModelSelectionTrials(),
                    nodeCount
                ));

            }}
        );
    }

    public static MemoryEstimation estimate(
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig configuration,
        ModelCatalog modelCatalog
    ) {
        PipelineExecutor.validateTrainingParameterSpace(pipeline);

        MemoryEstimation nodePropertyStepsEstimation = PipelineExecutor.estimateNodePropertySteps(
            modelCatalog,
            configuration.username(),
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            configuration.relationshipTypes()
        );

        var trainingEstimation = MemoryEstimations
            .builder()
            .add("Pipeline Train", NodeClassificationTrain.estimate(pipeline, configuration))
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
    protected NodeClassificationTrainPipelineResult execute(Map<DatasetSplits, GraphFilter> dataSplits) {
        PipelineExecutor.validateTrainingParameterSpace(pipeline);

        var nodeLabels = config.nodeLabelIdentifiers(graphStore);
        var nodesGraph = graphStore.getGraph(nodeLabels);

        this.pipeline.splitConfig().validateMinNumNodesInSplitSets(nodesGraph);

        var trainResult = NodeClassificationTrain
            .create(nodesGraph, pipeline, config, progressTracker, terminationFlag)
            .compute();

        var catalogModel = Model.of(
            config.username(),
            config.modelName(),
            NodeClassificationTrainingPipeline.MODEL_TYPE,
            schemaBeforeSteps,
            trainResult.classifier().data(),
            config,
            NodeClassificationPipelineModelInfo.of(
                trainResult.trainingStatistics().winningModelTestMetrics(),
                trainResult.trainingStatistics().winningModelOuterTrainMetrics(),
                trainResult.trainingStatistics().bestCandidate(),
                NodePropertyPredictPipeline.from(pipeline),
                trainResult.classifier().classIdMap().originalIdsList()
            )
        );

        return ImmutableNodeClassificationTrainPipelineResult.of(catalogModel, trainResult.trainingStatistics());
    }

    @ValueClass
    public interface NodeClassificationTrainPipelineResult {
        Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> model();
        TrainingStatistics trainingStatistics();
    }
}
