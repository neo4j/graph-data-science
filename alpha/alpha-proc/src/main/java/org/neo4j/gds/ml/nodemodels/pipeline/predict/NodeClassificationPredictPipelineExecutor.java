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
package org.neo4j.gds.ml.nodemodels.pipeline.predict;


import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredict;
import org.neo4j.gds.ml.pipeline.ImmutableGraphFilter;
import org.neo4j.gds.ml.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineTrainConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NodeClassificationPredictPipelineExecutor extends PipelineExecutor<
    NodeClassificationPredictPipelineBaseConfig,
    NodeClassificationPipeline,
    NodeClassificationPredict.NodeClassificationResult
    > {
    private static final int MIN_BATCH_SIZE = 100;
    private final LogisticRegressionData modelData;

    public NodeClassificationPredictPipelineExecutor(
        NodeClassificationPipeline pipeline,
        NodeClassificationPredictPipelineBaseConfig config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        String graphName,
        ProgressTracker progressTracker,
        LogisticRegressionData modelData
    ) {
        super(pipeline, config, executionContext, graphStore, graphName, progressTracker);
        this.modelData = modelData;
    }

    public static MemoryEstimation estimate(
        Model<LogisticRegressionData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> model,
        NodeClassificationPredictPipelineBaseConfig configuration,
        ModelCatalog modelCatalog
    ) {
        var pipeline = model.customInfo().trainingPipeline();
        var classCount = model.customInfo().classes().size();
        var featureCount = model.data().weights().data().cols();

        MemoryEstimation nodePropertyStepEstimation = PipelineExecutor.estimateNodePropertySteps(
            modelCatalog,
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            configuration.relationshipTypes()
        );

        var predictionEstimation = MemoryEstimations.builder().add(
            "Pipeline Predict",
            NodeClassificationPredict.memoryEstimationWithDerivedBatchSize(
                configuration.includePredictedProbabilities(),
                MIN_BATCH_SIZE,
                featureCount,
                classCount
            )
        ).build();

        return MemoryEstimations.maxEstimation(List.of(nodePropertyStepEstimation, predictionEstimation));
    }

    @Override
    public Map<DatasetSplits, GraphFilter> splitDataset() {
        // For prediction, we don't split the input graph but generate the features and predict over the whole graph
        return Map.of(
            DatasetSplits.FEATURE_INPUT,
            ImmutableGraphFilter.of(
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            )
        );
    }

    @Override
    protected NodeClassificationPredict.NodeClassificationResult execute(Map<DatasetSplits, GraphFilter> dataSplits) {
        var graph = graphStore.getGraph(
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore),
            Optional.empty()
        );
        var innerAlgo = new NodeClassificationPredict(
            new LogisticRegressionClassifier(modelData),
            graph,
            MIN_BATCH_SIZE,
            config.concurrency(),
            config.includePredictedProbabilities(),
            pipeline.featureProperties(),
            progressTracker
        );
        return innerAlgo.compute();
    }
}
