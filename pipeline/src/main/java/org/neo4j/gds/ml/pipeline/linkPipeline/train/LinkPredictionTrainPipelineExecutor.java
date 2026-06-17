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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.GdsVersionInfoProvider;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.model.CatalogModelContainer;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.pipeline.PipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.training.TrainingStatistics;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline.MODEL_TYPE;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionRelationshipSampler.splitEstimation;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainPipelineExecutor.LinkPredictionTrainPipelineResult;
import static org.neo4j.gds.ml.util.TrainingSetWarnings.warnForSmallRelationshipSets;

public class LinkPredictionTrainPipelineExecutor extends PipelineExecutor
    <LinkPredictionTrainConfig, LinkPredictionTrainingPipeline, LinkPredictionTrainPipelineResult> {
    private final Log log;

    private final LinkPredictionRelationshipSampler linkPredictionRelationshipSampler;

    private final Set<RelationshipType> availableRelationshipTypesForNodeProperty;

    public LinkPredictionTrainPipelineExecutor(
        Log log,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        LinkPredictionTrainingPipeline pipeline,
        LinkPredictionTrainConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore
    ) {
        super(progressTracker, terminationFlag, pipeline, config, executionContext, graphStore);
        this.log = log;

        this.availableRelationshipTypesForNodeProperty = graphStore.relationshipTypes()
            .stream()
            .filter(relType -> !relType.name.equals(config.targetRelationshipType()))
            .collect(Collectors.toSet());

        this.linkPredictionRelationshipSampler = new LinkPredictionRelationshipSampler(
            log,
            graphStore,
            pipeline.splitConfig(),
            config,
            progressTracker,
            terminationFlag
        );
    }

    public static Task progressTask(
        String taskName,
        Concurrency concurrency,
        LinkPredictionTrainingPipeline pipeline, long relationshipCount
    ) {
        var sizes = pipeline.splitConfig().expectedSetSizes(relationshipCount);
        List<Task> childTasks = new ArrayList<>();
        childTasks.add(LinkPredictionRelationshipSampler.progressTask(concurrency, sizes));
        childTasks.add(NodePropertyStepExecutor.tasks(
            concurrency,
            pipeline.nodePropertySteps(),
            sizes.featureInputSize()
        ));
        childTasks.addAll(LinkPredictionTrain.progressTasks(
            concurrency, relationshipCount,
            pipeline.splitConfig(),
            pipeline.numberOfModelSelectionTrials()
        ));

        return Tasks.task(taskName, concurrency, childTasks);
    }

    public static MemoryEstimation estimate(
        ModelCatalog modelCatalog,
        LinkPredictionTrainingPipeline pipeline,
        LinkPredictionTrainConfig configuration,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        String username
    ) {
        pipeline.validateTrainingParameterSpace();

        var splitEstimations = splitEstimation(
            pipeline.splitConfig(),
            configuration.targetRelationshipType(),
            pipeline.relationshipWeightProperty(modelCatalog, username)
        );

        MemoryEstimation maxOverNodePropertySteps = NodePropertyStepExecutor.estimateNodePropertySteps(
            algorithmsProcedureFacade,
            modelCatalog,
            configuration.username(),
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            List.of(pipeline.splitConfig().featureInputRelationshipType().name)
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
    public Map<DatasetSplits, PipelineGraphFilter> generateDatasetSplitGraphFilters() {
        var splitConfig = pipeline.splitConfig();

        return Map.of(
            DatasetSplits.TRAIN, new PipelineGraphFilter(config.nodeLabelIdentifiers(graphStore), List.of(splitConfig.trainRelationshipType())),
            DatasetSplits.TEST, new PipelineGraphFilter(config.nodeLabelIdentifiers(graphStore), List.of(splitConfig.testRelationshipType())),
            DatasetSplits.FEATURE_INPUT, new PipelineGraphFilter(config.nodeLabelIdentifiers(graphStore), List.of(splitConfig.featureInputRelationshipType()))
        );
    }

    @Override
    public void splitDatasets() {
        this.linkPredictionRelationshipSampler.splitAndSampleRelationships(
            pipeline.relationshipWeightProperty(executionContext.modelCatalog(), executionContext.user().getUsername())
        );
    }

    @Override
    protected LinkPredictionTrainPipelineResult execute(Map<DatasetSplits, PipelineGraphFilter> dataSplits) {
        pipeline.validateTrainingParameterSpace();

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
            log,
            trainGraph.relationshipCount(),
            testGraph.relationshipCount(),
            pipeline.splitConfig().validationFolds()
        );

        var trainResult = new LinkPredictionTrain(
            log,
            trainGraph,
            testGraph,
            pipeline,
            config,
            progressTracker,
            terminationFlag
        ).compute();

        var model = Model.of(
            GdsVersionInfoProvider.GDS_VERSION_INFO.gdsVersion(),
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


        return new LinkPredictionTrainPipelineResult(model, trainResult.trainingStatistics());
    }

    @Override
    protected Set<RelationshipType> getAvailableRelTypesForNodePropertySteps() {
        return availableRelationshipTypesForNodeProperty;
    }

    private void removeDataSplitRelationships(Map<DatasetSplits, PipelineGraphFilter> datasets) {
        datasets.values()
            .stream()
            .flatMap(graphFilter -> graphFilter.relationshipTypes().stream())
            .distinct()
            .toList()
            .forEach(graphStore::deleteRelationships);
    }

    @Override
    protected void additionalGraphStoreCleanup(Map<DatasetSplits, PipelineGraphFilter> datasets) {
        removeDataSplitRelationships(datasets);
        super.additionalGraphStoreCleanup(datasets);
    }

    public record LinkPredictionTrainPipelineResult(
        Model<Classifier.ClassifierData, LinkPredictionTrainConfig, LinkPredictionModelInfo> model,
        TrainingStatistics trainingStatistics
    ) implements CatalogModelContainer<Classifier.ClassifierData, LinkPredictionTrainConfig, LinkPredictionModelInfo> {}
}
