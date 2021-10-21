/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrain;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;

import java.util.Collection;
import java.util.Optional;

public class NodeClassificationPipelineTrain extends Algorithm<NodeClassificationPipelineTrain, Model<NodeLogisticRegressionData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo>> {
    public static final String MODEL_TYPE = "Node classification pipeline";

    private final NodeClassificationPipelineTrainConfig trainConfig;
    private final GraphStore graphStore;
    private final String graphName;
    private final BaseProc caller;
    private final String userName;
    private final AllocationTracker allocationTracker;

    NodeClassificationPipelineTrain(
        NodeClassificationPipelineTrainConfig trainConfig,
        GraphStore graphStore,
        String graphName, AllocationTracker allocationTracker,
        ProgressTracker progressTracker,
        BaseProc caller,
        String userName
    ) {
        super(progressTracker);
        this.trainConfig = trainConfig;
        this.graphStore = graphStore;
        this.graphName = graphName;
        this.allocationTracker = allocationTracker;
        this.caller = caller;
        this.userName = userName;
    }

    @Override
    public Model<NodeLogisticRegressionData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> compute() {
        var nodeLabels = trainConfig.nodeLabelIdentifiers(graphStore);
        var relationshipTypes = trainConfig.internalRelationshipTypes(graphStore);
        executeNodePropertySteps(nodeLabels, relationshipTypes);

        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, Optional.empty());
        var innerModel = NodeClassificationTrain
            .create(graph, innerConfig(), allocationTracker, progressTracker)
            .compute();

        var innerInfo = innerModel.customInfo();

        var modelInfo = NodeClassificationPipelineModelInfo.builder()
            .classes(innerInfo.classes())
            .bestParameters(innerInfo.bestParameters())
            .metrics(innerInfo.metrics())
            .trainingPipeline(getInfo())
            .build();

        return Model.of(
            innerModel.creator(),
            innerModel.name(),
            MODEL_TYPE,
            innerModel.graphSchema(),
            innerModel.data(),
            trainConfig,
            modelInfo
        );
    }

    public NodeClassificationTrainConfig innerConfig() {
        var nodeClassificationPipelineInfo = getInfo();
        return org.neo4j.gds.ml.nodemodels.ImmutableNodeClassificationTrainConfig.builder()
                .modelName(trainConfig.modelName())
                .concurrency(trainConfig.concurrency())
                .metrics(trainConfig.metrics())
                .targetProperty(trainConfig.targetProperty())
                .featureProperties(nodeClassificationPipelineInfo.featureProperties())
                .params(nodeClassificationPipelineInfo.parameterSpace())
                .holdoutFraction(nodeClassificationPipelineInfo.splitConfig().holdoutFraction())
                .validationFolds(nodeClassificationPipelineInfo.splitConfig().validationFolds())
                .build();
    }

    private NodeClassificationTrainingPipeline getInfo() {
        String pipeline = trainConfig.pipeline();
        var model = OpenModelCatalog.INSTANCE.getUntyped(userName, pipeline);

        //TODO: are asserts necessary?
        assert model != null;
        //TODO: here we had before some model type validation. figure out where that validation should be
        assert model.customInfo() instanceof NodeClassificationTrainingPipeline;

        return (NodeClassificationTrainingPipeline) model.customInfo();
    }

    private void executeNodePropertySteps(Collection<NodeLabel> nodeLabels, Collection<RelationshipType> relationshipTypes) {
        getInfo()
            .nodePropertySteps()
            .forEach(step -> step.execute(caller, graphName, nodeLabels, relationshipTypes));
    }

    @Override
    public NodeClassificationPipelineTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}
