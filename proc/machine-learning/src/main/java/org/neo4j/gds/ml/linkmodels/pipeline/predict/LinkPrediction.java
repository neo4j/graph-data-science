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
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;

public abstract class LinkPrediction {

    static final int MIN_NODE_BATCH_SIZE = 10;
    private final Classifier classifier;
    private final LinkFeatureExtractor linkFeatureExtractor;
    protected final Graph graph;

    protected final LPNodeFilter sourceNodeFilter;

    protected final LPNodeFilter targetNodeFilter;

    protected final int concurrency;
    final ProgressTracker progressTracker;

    LinkPrediction(
        Classifier classifier,
        LinkFeatureExtractor linkFeatureExtractor,
        Graph graph,
        LPNodeFilter sourceNodeFilter,
        LPNodeFilter targetNodeFilter,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        this.classifier = classifier;
        this.linkFeatureExtractor = linkFeatureExtractor;
        this.graph = graph;
        this.sourceNodeFilter = sourceNodeFilter;
        this.targetNodeFilter = targetNodeFilter;
        this.concurrency = concurrency;
        this.progressTracker = progressTracker;
    }

    public LinkPredictionResult compute() {
        progressTracker.beginSubTask();

        var result = predict();

        progressTracker.endSubTask();

        return result;
    }

    private LinkPredictionResult predict() {
        var linkPredictionSimilarityComputer = new LinkPredictionSimilarityComputer(
            linkFeatureExtractor,
            classifier
        );

        return predictLinks(linkPredictionSimilarityComputer);
    }

    abstract LinkPredictionResult predictLinks(
        LinkPredictionSimilarityComputer linkPredictionSimilarityComputer
    );
}
