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
import org.neo4j.gds.models.FeaturesFactory;
import org.neo4j.gds.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.gds.similarity.knn.NeighborFilter;
import org.neo4j.gds.similarity.knn.NeighborFilterFactory;
import org.neo4j.gds.similarity.knn.SimilarityComputer;

class LinkPredictionSimilarityComputer implements SimilarityComputer {

    private final LinkFeatureExtractor linkFeatureExtractor;
    private final LogisticRegressionClassifier classifier;
    private final int positiveClassLocalId;

    LinkPredictionSimilarityComputer(
        LinkFeatureExtractor linkFeatureExtractor,
        LogisticRegressionClassifier classifier
    ) {
        this.linkFeatureExtractor = linkFeatureExtractor;
        this.classifier = classifier;
        this.positiveClassLocalId = classifier.classIdMap().toMapped((long)EdgeSplitter.POSITIVE);
    }

    @Override
    public double similarity(long sourceId, long targetId) {
        var features = linkFeatureExtractor.extractFeatures(sourceId, targetId);
        return classifier.predictProbabilities(0L, FeaturesFactory.wrap(features))[positiveClassLocalId];
    }

    static final class LinkFilter implements NeighborFilter {

        private final Graph graph;

        private LinkFilter(Graph graph) {
            this.graph = graph;
        }

        @Override
        public boolean excludeNodePair(long firstNodeId, long secondNodeId) {
            if (firstNodeId == secondNodeId) {
                return true;
            }

            // This is a slower but memory-efficient approach (could be replaced by a dedicated data structure)
            return graph.exists(firstNodeId, secondNodeId);
        }

        @Override
        public long lowerBoundOfPotentialNeighbours(long node) {
            return graph.nodeCount() - 1 - graph.degree(node);
        }
    }

    static class LinkFilterFactory implements NeighborFilterFactory {

        private final Graph graph;

        LinkFilterFactory(Graph graph) {
            this.graph = graph;
        }

        @Override
        public NeighborFilter create() {
            return new LinkPredictionSimilarityComputer.LinkFilter(graph.concurrentCopy());
        }
    }
}
