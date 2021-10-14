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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.linkmodels.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;

import java.util.Collection;
import java.util.Optional;

public abstract class LinkPrediction extends Algorithm<LinkPrediction, LinkPredictionResult> {

    private final LinkLogisticRegressionData modelData;
    private final PipelineExecutor pipelineExecutor;
    private final Collection<NodeLabel> nodeLabels;
    private final Collection<RelationshipType> relationshipTypes;
    private final GraphStore graphStore;
    protected final int concurrency;

    LinkPrediction(
        LinkLogisticRegressionData modelData,
        PipelineExecutor pipelineExecutor,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        GraphStore graphStore,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);

        this.modelData = modelData;
        this.pipelineExecutor = pipelineExecutor;
        this.nodeLabels = nodeLabels;
        this.relationshipTypes = relationshipTypes;
        this.graphStore = graphStore;
        this.concurrency = concurrency;
    }

    @Override
    public LinkPredictionResult compute() {
        progressTracker.beginSubTask();
        pipelineExecutor.executeNodePropertySteps(nodeLabels, relationshipTypes);
        assertRunning();

        var result = predict();

        pipelineExecutor.removeNodeProperties(graphStore, nodeLabels);
        progressTracker.endSubTask();

        return result;
    }

    private LinkPredictionResult predict() {
        progressTracker.beginSubTask();
        // retrieve the graph containing the node-properties added by executing the node property steps
        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, Optional.empty());
        var featureExtractor = pipelineExecutor.linkFeatureExtractor(graph);

        assert featureExtractor.featureDimension() == modelData
            .weights()
            .data()
            .totalSize() : "Model must contain a weight for each feature.";

        var predictor = new LinkLogisticRegressionPredictor(modelData);

        var linkPredictionSimilarityComputer = new LinkPredictionSimilarityComputer(
            featureExtractor,
            predictor,
            graph
        );

        var result = predictLinks(graph, linkPredictionSimilarityComputer);
        progressTracker.endSubTask();
        return result;
    }

    abstract LinkPredictionResult predictLinks(
        Graph graph,
        LinkPredictionSimilarityComputer linkPredictionSimilarityComputer
    );

    @Override
    public LinkPrediction me() {
        return this;
    }

    @Override
    public void release() {

    }
}
