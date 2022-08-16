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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.neo4j.gds.RelationshipType;
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
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrain;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.training.TrainingStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.linkmodels.pipeline.train.LinkPredictionTrainPipelineExecutor.LinkPredictionTrainPipelineResult;
import static org.neo4j.gds.ml.linkmodels.pipeline.train.RelationshipSplitter.splitEstimation;
import static org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline.MODEL_TYPE;
import static org.neo4j.gds.ml.util.TrainingSetWarnings.warnForSmallRelationshipSets;

public class LinkPredictionTrainPipelineExecutor extends PipelineExecutor
    <LinkPredictionTrainConfig, LinkPredictionTrainingPipeline, LinkPredictionTrainPipelineResult> {

    private final RelationshipSplitter relationshipSplitter;

    public LinkPredictionTrainPipelineExecutor(
        LinkPredictionTrainingPipeline pipeline,
        LinkPredictionTrainConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        String graphName,
        ProgressTracker progressTracker
    ) {
        super(
            pipeline,
            config,
            executionContext,
            graphStore,
            graphName,
            progressTracker
        );

        this.relationshipSplitter = new RelationshipSplitter(
            graphName,
            pipeline.splitConfig(),
            executionContext,
            progressTracker
        );
    }

    public static Task progressTask(String taskName, LinkPredictionTrainingPipeline pipeline, long relationshipCount) {
        var sizes = pipeline.splitConfig().expectedSetSizes(relationshipCount);
        return Tasks.task(taskName, new ArrayList<>() {{
            add(Tasks.leaf(
                "Split relationships",
                sizes.trainSize() + sizes.featureInputSize() + sizes.testSize() + sizes.testComplementSize()
            ));
            add(nodePropertyStepTasks(pipeline.nodePropertySteps(), sizes.featureInputSize()));
            addAll(LinkPredictionTrain.progressTasks(
                relationshipCount,
                pipeline.splitConfig(),
                pipeline.numberOfModelSelectionTrials()
            ));
        }});
    }

    public static MemoryEstimation estimate(
        ModelCatalog modelCatalog,
        LinkPredictionTrainingPipeline pipeline,
        LinkPredictionTrainConfig configuration
    ) {
        PipelineExecutor.validateTrainingParameterSpace(pipeline);

        var splitEstimations = splitEstimation(pipeline.splitConfig(), configuration.relationshipTypes());

        MemoryEstimation maxOverNodePropertySteps = PipelineExecutor.estimateNodePropertySteps(
            modelCatalog,
            configuration.username(),
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            List.of(pipeline.splitConfig().featureInputRelationshipType())
        );

        MemoryEstimation trainingEstimation = MemoryEstimations
            .builder()
            .add("Train pipeline", LinkPredictionTrain.estimate(pipeline, configuration))
            .build();

        return MemoryEstimations.builder(LinkPredictionTrainPipelineExecutor.class.getSimpleName())
            .max("Pipeline execution", List.of(splitEstimations, maxOverNodePropertySteps, trainingEstimation))
            .build();
    }

    @Override
    public Map<DatasetSplits, PipelineExecutor.GraphFilter> splitDataset() {
        this.relationshipSplitter.splitRelationships(
            graphStore,
            config.relationshipTypes(),
            config.nodeLabels(),
            config.randomSeed(),
            pipeline.relationshipWeightProperty(executionContext)
        );

        var splitConfig = pipeline.splitConfig();

        var nodeLabels = config.nodeLabelIdentifiers(graphStore);
        var trainRelationshipTypes = RelationshipType.listOf(splitConfig.trainRelationshipType());
        var testRelationshipTypes = RelationshipType.listOf(splitConfig.testRelationshipType());
        var featureInputRelationshipType = RelationshipType.listOf(splitConfig.featureInputRelationshipType());

        return Map.of(
            DatasetSplits.TRAIN, ImmutableGraphFilter.of(nodeLabels, trainRelationshipTypes),
            DatasetSplits.TEST, ImmutableGraphFilter.of(nodeLabels, testRelationshipTypes),
            DatasetSplits.FEATURE_INPUT, ImmutableGraphFilter.of(nodeLabels, featureInputRelationshipType)
        );
    }

    @Override
    protected LinkPredictionTrainPipelineResult execute(Map<DatasetSplits, GraphFilter> dataSplits) {
        PipelineExecutor.validateTrainingParameterSpace(pipeline);

        var trainDataSplit = dataSplits.get(DatasetSplits.TRAIN);
        var testDataSplit = dataSplits.get(DatasetSplits.TEST);

        var trainGraph = graphStore.getGraph(
            trainDataSplit.nodeLabels(),
            trainDataSplit.relationshipTypes(),
            Optional.of("label")
        );
        var testGraph = graphStore.getGraph(
            testDataSplit.nodeLabels(),
            testDataSplit.relationshipTypes(),
            Optional.of("label")
        );

        warnForSmallRelationshipSets(
            trainGraph.relationshipCount(),
            testGraph.relationshipCount(),
            pipeline.splitConfig().validationFolds(),
            progressTracker
        );

        var trainResult = new LinkPredictionTrain(
            trainGraph,
            testGraph,
            pipeline,
            config,
            progressTracker,
            terminationFlag
        ).compute();

        var model = Model.of(
            config.username(),
            config.modelName(),
            MODEL_TYPE,
            schemaBeforeSteps,
            trainResult.classifier().data(),
            config,
            LinkPredictionModelInfo.of(
                trainResult.trainingStatistics().winningModelTestMetrics(),
                trainResult.trainingStatistics().winningModelOuterTrainMetrics(),
                trainResult.trainingStatistics().bestCandidate(),
                LinkPredictionPredictPipeline.from(pipeline)
            )
        );


        return ImmutableLinkPredictionTrainPipelineResult.of(model, trainResult.trainingStatistics());
    }

    private void removeDataSplitRelationships(Map<DatasetSplits, GraphFilter> datasets) {
        datasets.values()
            .stream()
            .flatMap(graphFilter -> graphFilter.relationshipTypes().stream())
            .distinct()
            .collect(Collectors.toList())
            .forEach(graphStore::deleteRelationships);
    }

    @Override
    protected void cleanUpGraphStore(Map<DatasetSplits, GraphFilter> datasets) {
        removeDataSplitRelationships(datasets);
        super.cleanUpGraphStore(datasets);
    }

    @ValueClass
    public interface LinkPredictionTrainPipelineResult {
        Model<Classifier.ClassifierData, LinkPredictionTrainConfig, LinkPredictionModelInfo> model();

        TrainingStatistics trainingStatistics();
    }
}
