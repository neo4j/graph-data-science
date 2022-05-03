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
package org.neo4j.gds.ml.pipeline.nodePipeline.regression;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.pipeline.ImmutableGraphFilter;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.pipeline.TrainingStatistics;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainPipelineExecutor.NodeRegressionTrainPipelineResult;

public class NodeRegressionTrainPipelineExecutor extends PipelineExecutor<
    NodeRegressionPipelineTrainConfig,
    NodeRegressionTrainingPipeline,
    NodeRegressionTrainPipelineResult
> {

    public static Task progressTask(NodeRegressionTrainingPipeline pipeline) {
        return Tasks.task(
            "Node Regression Train Pipeline",
            new ArrayList<>() {{
                add(Tasks.iterativeFixed(
                    "Execute node property steps",
                    () -> List.of(Tasks.leaf("Step")),
                    pipeline.nodePropertySteps().size()
                ));
                addAll(NodeRegressionTrain.progressTasks(
                    pipeline.splitConfig().validationFolds(),
                    pipeline.numberOfModelSelectionTrials()
                ));

            }}
        );
    }

    public NodeRegressionTrainPipelineExecutor(
        NodeRegressionTrainingPipeline pipeline,
        NodeRegressionPipelineTrainConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        ProgressTracker progressTracker
    ) {
        super(pipeline, config, executionContext, graphStore, config.graphName(), progressTracker);
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
    protected NodeRegressionTrainPipelineResult execute(Map<DatasetSplits, GraphFilter> dataSplits) {
        PipelineExecutor.validateTrainingParameterSpace(pipeline);

        var nodeLabels = config.nodeLabelIdentifiers(graphStore);
        var relationshipTypes = config.internalRelationshipTypes(graphStore);
        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, Optional.empty());

        this.pipeline.splitConfig().validateMinNumNodesInSplitSets(graph);

        NodeRegressionTrainResult trainResult = NodeRegressionTrain
            .create(graph, pipeline, config, progressTracker, terminationFlag)
            .compute();

        var catalogModel = Model.of(
            config.username(),
            config.modelName(),
            NodeRegressionTrainingPipeline.MODEL_TYPE,
            schemaBeforeSteps,
            trainResult.regressor().data(),
            config,
            NodeRegressionPipelineModelInfo.builder()
                .bestParameters(trainResult.trainingStatistics().bestParameters())
                .metrics(trainResult.trainingStatistics().metricsForWinningModel())
                .pipeline(NodePropertyPredictPipeline.from(pipeline))
                .build()
        );

        return ImmutableNodeRegressionTrainPipelineResult.of(catalogModel, trainResult.trainingStatistics());
    }

    @ValueClass
    public interface NodeRegressionTrainPipelineResult {
        Model<Regressor.RegressorData, NodeRegressionPipelineTrainConfig, NodeRegressionPipelineModelInfo> model();
        TrainingStatistics trainingStatistics();
    }
}
