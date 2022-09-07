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
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.ClassifierFactory;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.pipeline.ImmutablePipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.PipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.PredictPipelineExecutor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.similarity.knn.KnnFactory;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LinkPredictionPredictPipelineExecutor extends PredictPipelineExecutor<
    LinkPredictionPredictPipelineBaseConfig,
    LinkPredictionPredictPipeline,
    LinkPredictionResult
    > {
    private final Classifier classifier;

    private final LPGraphStoreFilter graphStoreFilter;

    public LinkPredictionPredictPipelineExecutor(
        LinkPredictionPredictPipeline pipeline,
        Classifier classifier,
        LPGraphStoreFilter graphStoreFilter,
        LinkPredictionPredictPipelineBaseConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        ProgressTracker progressTracker
    ) {
        super(pipeline, config, executionContext, graphStore, progressTracker);
        this.classifier = classifier;
        this.graphStoreFilter = graphStoreFilter;
    }

    LPGraphStoreFilter labelFilter() {
        return graphStoreFilter;
    }

    @Override
    protected LinkPredictionResult execute() {
        var graph = graphStore.getGraph(
            graphStoreFilter.predictNodeLabels(),
            graphStoreFilter.predictRelationshipTypes(),
            Optional.empty()
        );

        var linkFeatureExtractor = LinkFeatureExtractor.of(graph, pipeline.featureSteps());
        var linkPrediction = getLinkPredictionStrategy(graph, config.isApproximateStrategy(), linkFeatureExtractor);
        return linkPrediction.compute();
    }

    @Override
    protected PipelineGraphFilter nodePropertyStepFilter() {
        return ImmutablePipelineGraphFilter.builder()
            .nodeLabels(graphStoreFilter.nodePropertyStepsBaseLabels())
            .relationshipTypes(graphStoreFilter.predictRelationshipTypes())
            .build();
    }

    public static Task progressTask(
        String taskName,
        LinkPredictionPredictPipeline pipeline,
        GraphStore graphStore,
        LinkPredictionPredictPipelineBaseConfig config
    ) {
        return Tasks.task(
            taskName,
            NodePropertyStepExecutor.tasks(pipeline.nodePropertySteps(), graphStore.relationshipCount()),
            config.isApproximateStrategy()
                ? Tasks.task(
                "Approximate link prediction",
                KnnFactory.knnTaskTree(graphStore.getUnion(), config.approximateConfig())
            )
                : Tasks.leaf("Exhaustive link prediction", graphStore.getUnion().nodeCount() * graphStore.getUnion().nodeCount() / 2)
        );
    }

    public static MemoryEstimation estimate(
        ModelCatalog modelCatalog,
        LinkPredictionPredictPipeline pipeline,
        LinkPredictionPredictPipelineBaseConfig configuration,
        Classifier.ClassifierData classifierData
    ) {
        MemoryEstimation maxOverNodePropertySteps = NodePropertyStepExecutor.estimateNodePropertySteps(
            modelCatalog,
            configuration.username(),
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            configuration.relationshipTypes()
        );

        var strategyEstimation = configuration.isApproximateStrategy()
            ? ApproximateLinkPrediction.estimate(configuration)
            : ExhaustiveLinkPrediction.estimate(configuration, classifierData.featureDimension());

        MemoryRange classificationRange;
        // LR prediction requires no computation graph overhead in the binary case.
        if (classifierData.trainerMethod() == TrainingMethod.LogisticRegression) {
            classificationRange = MemoryRange.of(0);
        } else {
            classificationRange = ClassifierFactory.runtimeOverheadMemoryEstimation(
                classifierData.trainerMethod(),
                1,
                2,
                classifierData.featureDimension(),
                true
            );
        }
        var predictEstimation = MemoryEstimations.builder("Model prediction")
            .add("Strategy runtime", strategyEstimation)
            .add(MemoryEstimations.of("Classifier runtime", classificationRange))
            .build();

        return MemoryEstimations.builder(LinkPredictionPredictPipelineExecutor.class.getSimpleName())
            .max("Pipeline execution", List.of(maxOverNodePropertySteps, predictEstimation))
            .build();
    }

    private LinkPrediction getLinkPredictionStrategy(
        Graph graph,
        boolean isApproximateStrategy,
        LinkFeatureExtractor linkFeatureExtractor
    ) {
        if (linkFeatureExtractor.featureDimension() != classifier.data().featureDimension()) {
            var inputNodeProperties = pipeline
                .featureSteps()
                .stream()
                .flatMap(step -> step.inputNodeProperties().stream())
                .collect(Collectors.toSet());

            throw new IllegalArgumentException(formatWithLocale(
                "Model expected link features to have a total dimension of `%d`, but got `%d`." +
                " This indicates the dimension of the node-properties %s differ between the input and the original train graph.",
                classifier.data().featureDimension(),
                linkFeatureExtractor.featureDimension(),
                StringJoining.join(inputNodeProperties)
            ));
        }

        IdMap sourceNodes = graphStore.getGraph(graphStoreFilter.sourceNodeLabels());
        IdMap targetNodes = graphStore.getGraph(graphStoreFilter.targetNodeLabels());

        var sourceNodeFilter = LPNodeFilter.of(graph, sourceNodes);
        var targetNodeFilter = LPNodeFilter.of(graph, targetNodes);

        if (isApproximateStrategy) {
            return new ApproximateLinkPrediction(
                classifier,
                linkFeatureExtractor,
                graph,
                sourceNodeFilter,
                targetNodeFilter,
                config.approximateConfig(),
                progressTracker,
                terminationFlag
            );
        } else {
            return new ExhaustiveLinkPrediction(
                classifier,
                linkFeatureExtractor,
                graph,
                sourceNodeFilter,
                targetNodeFilter,
                config.concurrency(),
                config.topN().orElseThrow(),
                config.thresholdOrDefault(),
                progressTracker,
                terminationFlag
            );
        }
    }

}
