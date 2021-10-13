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

import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;
import org.neo4j.gds.similarity.knn.SimilarityComputer;

class LinkPredictionSimilarityComputer implements SimilarityComputer {

    private final LinkFeatureExtractor linkFeatureExtractor;
    private final LinkLogisticRegressionPredictor predictor;

    LinkPredictionSimilarityComputer(
        LinkFeatureExtractor linkFeatureExtractor,
        LinkLogisticRegressionPredictor predictor
    ) {
        this.linkFeatureExtractor = linkFeatureExtractor;
        this.predictor = predictor;
    }

    @Override
    public double similarity(long sourceId, long targetId) {
        var features = linkFeatureExtractor.extractFeatures(sourceId, targetId);
        return predictor.predictedProbability(features);
    }

    @Override
    public boolean filterNodePair(long firstNodeId, long secondNodeId) {
        return firstNodeId < secondNodeId;
    }
}
