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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;

public abstract class LinkPrediction extends Algorithm<LinkPrediction, LinkPredictionResult> {

    private final LinkLogisticRegressionData modelData;
    private final LinkFeatureExtractor linkFeatureExtractor;
    private final Graph graph;
    protected final int concurrency;

    LinkPrediction(
        LinkLogisticRegressionData modelData,
        LinkFeatureExtractor linkFeatureExtractor,
        Graph graph,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);

        this.modelData = modelData;
        this.linkFeatureExtractor = linkFeatureExtractor;
        this.graph = graph;
        this.concurrency = concurrency;
    }

    @Override
    public LinkPredictionResult compute() {
        progressTracker.beginSubTask();

        var result = predict();

        progressTracker.endSubTask();

        return result;
    }

    private LinkPredictionResult predict() {
        assert linkFeatureExtractor.featureDimension() == modelData
            .weights()
            .data()
            .totalSize() : "Model must contain a weight for each feature.";

        var predictor = new LinkLogisticRegressionPredictor(modelData);

        var linkPredictionSimilarityComputer = new LinkPredictionSimilarityComputer(
            linkFeatureExtractor,
            predictor,
            graph
        );

        return predictLinks(graph, linkPredictionSimilarityComputer);
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
