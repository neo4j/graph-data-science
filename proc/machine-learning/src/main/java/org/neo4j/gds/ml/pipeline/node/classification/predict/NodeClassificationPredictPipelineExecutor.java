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
package org.neo4j.gds.ml.pipeline.node.classification.predict;


import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.ClassifierFactory;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.nodeClassification.NodeClassificationPredict;
import org.neo4j.gds.ml.pipeline.ImmutableGraphFilter;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeClassificationPredictPipelineExecutor extends PipelineExecutor<
    NodeClassificationPredictPipelineBaseConfig,
    NodePropertyPredictPipeline,
    NodeClassificationPredictPipelineExecutor.NodeClassificationPipelineResult
    > {
    private static final int MIN_BATCH_SIZE = 100;
    private final Classifier.ClassifierData modelData;
    private final LocalIdMap classIdMap;

    public NodeClassificationPredictPipelineExecutor(
        NodePropertyPredictPipeline pipeline,
        NodeClassificationPredictPipelineBaseConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        ProgressTracker progressTracker,
        Classifier.ClassifierData modelData,
        LocalIdMap classIdMap
    ) {
        super(pipeline, config, executionContext, graphStore, progressTracker);
        this.modelData = modelData;
        this.classIdMap = classIdMap;
    }

    public static Task progressTask(String taskName, NodePropertyPredictPipeline pipeline, GraphStore graphStore) {
        return Tasks.task(
            taskName,
            NodePropertyStepExecutor.tasks(pipeline.nodePropertySteps(), graphStore.nodeCount()),
            NodeClassificationPredict.progressTask(graphStore.nodeCount())
        );
    }

    public static MemoryEstimation estimate(
        Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> model,
        NodeClassificationPredictPipelineBaseConfig configuration,
        ModelCatalog modelCatalog
    ) {
        var pipeline = model.customInfo().pipeline();
        var classCount = model.customInfo().classes().size();
        var featureCount = model.data().featureDimension();

        MemoryEstimation nodePropertyStepEstimation = NodePropertyStepExecutor.estimateNodePropertySteps(
            modelCatalog,
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            configuration.relationshipTypes()
        );

        var predictionEstimation = MemoryEstimations.builder().add(
            "Pipeline Predict",
            NodeClassificationPredict.memoryEstimationWithDerivedBatchSize(
                model.data().trainerMethod(),
                configuration.includePredictedProbabilities(),
                MIN_BATCH_SIZE,
                featureCount,
                classCount,
                false
            )
        ).build();

        return MemoryEstimations.maxEstimation(List.of(nodePropertyStepEstimation, predictionEstimation));
    }

    @Override
    public Map<DatasetSplits, GraphFilter> generateDatasetSplitGraphFilters() {
        // For prediction, we don't split the input graph but generate the features and predict over the whole graph
        return Map.of(
            DatasetSplits.FEATURE_INPUT,
            ImmutableGraphFilter.builder()
                .nodeLabels(config.nodeLabelIdentifiers(graphStore))
                .contextRelationshipTypes(config.internalRelationshipTypes(graphStore)).build()
        );
    }

    @Override
    public void splitDatasets() {
        //NodeClassification Predict does not split datasets
    }

    @Override
    protected NodeClassificationPipelineResult execute(Map<DatasetSplits, GraphFilter> dataSplits) {
        var nodesGraph = graphStore.getGraph(config.nodeLabelIdentifiers(graphStore));
        var features = FeaturesFactory.extractLazyFeatures(nodesGraph, pipeline.featureProperties());

        if (features.featureDimension() != modelData.featureDimension()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Model expected features %s to have a total dimension of `%d`, but got `%d`.",
                StringJoining.join(pipeline.featureProperties()),
                modelData.featureDimension(),
                features.featureDimension()
            ));
        }

        var nodeClassificationResult =  new NodeClassificationPredict(
            ClassifierFactory.create(modelData),
            features,
            MIN_BATCH_SIZE,
            config.concurrency(),
            config.includePredictedProbabilities(),
            progressTracker,
            terminationFlag
        ).compute();

        return NodeClassificationPipelineResult.of(nodeClassificationResult, classIdMap);

    }

    @ValueClass
    public interface NodeClassificationPipelineResult {
        HugeLongArray predictedClasses();
        Optional<HugeObjectArray<double[]>> predictedProbabilities();

        static NodeClassificationPipelineResult of(NodeClassificationPredict.NodeClassificationResult nodeClassificationResult, LocalIdMap classIdMap) {
            var internalPredictions = nodeClassificationResult.predictedClasses();
            var predictions = HugeLongArray.newArray(internalPredictions.size());
            for (long i = 0; i < nodeClassificationResult.predictedClasses().size(); i++) {
                predictions.set(i, classIdMap.toOriginal(internalPredictions.get(i)));
            }
            return ImmutableNodeClassificationPipelineResult.of(predictions, nodeClassificationResult.predictedProbabilities());
        }
    }
}
