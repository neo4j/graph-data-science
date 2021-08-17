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

import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.linkmodels.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.LongStream;

public class LinkPrediction extends Algorithm<LinkPrediction, LinkPredictionResult> {

    private final LinkLogisticRegressionData modelData;
    private final PipelineExecutor pipelineExecutor;
    private final Collection<NodeLabel> nodeLabels;
    private final Collection<RelationshipType> relationshipTypes;
    private final GraphStore graphStore;
    private final int concurrency;
    private final int topN;
    private final double threshold;

    public LinkPrediction(
        LinkLogisticRegressionData modelData,
        PipelineExecutor pipelineExecutor,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        GraphStore graphStore,
        int concurrency,
        int topN,
        double threshold,
        ProgressTracker progressTracker
    ) {
        this.modelData = modelData;
        this.pipelineExecutor = pipelineExecutor;
        this.nodeLabels = nodeLabels;
        this.relationshipTypes = relationshipTypes;
        this.graphStore = graphStore;
        this.concurrency = concurrency;
        this.topN = topN;
        this.threshold = threshold;
        this.progressTracker = progressTracker;
    }

    @Override
    public LinkPredictionResult compute() {
        computeNodeProperties();
        return predictLinks();
    }

    private void computeNodeProperties() {
        pipelineExecutor.executeNodePropertySteps(nodeLabels, relationshipTypes);
    }

    private LinkPredictionResult predictLinks() {
        var graph = graphStore.getGraph(nodeLabels, relationshipTypes, Optional.empty());
        var featureExtractor = pipelineExecutor.linkFeatureExtractor(graph);

        assert featureExtractor.featureDimension() == modelData.weights().data().totalSize() : "Model must contain a weight for each feature.";

        var predictor = new LinkLogisticRegressionPredictor(modelData);
        var result = new LinkPredictionResult(topN);
        var batchQueue = new BatchQueue(graph.nodeCount(), BatchQueue.DEFAULT_BATCH_SIZE, concurrency);

        batchQueue.parallelConsume(concurrency, ignore -> new LinkPredictionScoreByIdsConsumer(
            graph.concurrentCopy(),
            featureExtractor,
            predictor,
            result,
            progressTracker
        ));

        pipelineExecutor.removeNodeProperties(graphStore, nodeLabels);

        return result;
    }

    @Override
    public LinkPrediction me() {
        return this;
    }

    @Override
    public void release() {

    }

    private final class LinkPredictionScoreByIdsConsumer implements Consumer<Batch> {
        private final Graph graph;
        private final LinkFeatureExtractor linkFeatureExtractor;
        private final LinkLogisticRegressionPredictor predictor;
        private final LinkPredictionResult predictedLinks;
        private final ProgressTracker progressTracker;

        private LinkPredictionScoreByIdsConsumer(
            Graph graph,
            LinkFeatureExtractor linkFeatureExtractor,
            LinkLogisticRegressionPredictor predictor,
            LinkPredictionResult predictedLinks,
            ProgressTracker progressTracker
        ) {
            this.graph = graph;
            this.linkFeatureExtractor = linkFeatureExtractor;
            this.predictor = predictor;
            this.predictedLinks = predictedLinks;
            this.progressTracker = progressTracker;
        }

        @Override
        public void accept(Batch batch) {
            for (long sourceId : batch.nodeIds()) {
                var largerNeighbors = largerNeighbors(sourceId);
                // since graph is undirected, only process pairs where sourceId < targetId
                var smallestTarget = sourceId + 1;
                LongStream.range(smallestTarget, graph.nodeCount()).forEach(targetId -> {
                        if (largerNeighbors.contains(targetId)) return;
                        var features = linkFeatureExtractor.extractFeatures(sourceId, targetId);
                        var probability = predictor.predictedProbability(features);
                        if (probability < threshold) return;
                        predictedLinks.add(sourceId, targetId, probability);
                    }
                );
            }
        }

        private LongHashSet largerNeighbors(long sourceId) {
            var neighbors = new LongHashSet();
            graph.forEachRelationship(
                sourceId, (src, trg) -> {
                    if (src < trg) neighbors.add(trg);
                    return true;
                }
            );
            return neighbors;
        }
    }
}
