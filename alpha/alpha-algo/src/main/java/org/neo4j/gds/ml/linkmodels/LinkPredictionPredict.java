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
package org.neo4j.gds.ml.linkmodels;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryUsage;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.BoundedLongLongPriorityQueue;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionPredictor;

import java.util.HashSet;
import java.util.function.Consumer;
import java.util.stream.LongStream;

public class LinkPredictionPredict extends Algorithm<LinkPredictionPredict, LinkPredictionResult> {

    private final LinkLogisticRegressionPredictor predictor;
    private final Graph graph;
    private final int batchSize;
    private final int concurrency;
    private final int topN;
    private final double threshold;

    public static MemoryEstimation memoryEstimation(int topN, int linkFeatureDimension, int nodeFeatureDimension) {
        var builder = MemoryEstimations.builder(LinkPredictionPredict.class);
        builder.add("TopN predictions", BoundedLongLongPriorityQueue.memoryEstimation(topN));
        builder.fixed("node feature vectors", 2 * MemoryUsage.sizeOfDoubleArray(nodeFeatureDimension));
        builder.fixed("link feature vector", MemoryUsage.sizeOfDoubleArray(linkFeatureDimension));
        return builder.build();
    }

    public LinkPredictionPredict(
        LinkLogisticRegressionPredictor predictor,
        Graph graph,
        int batchSize,
        int concurrency,
        int topN,
        ProgressTracker progressTracker,
        double threshold
    ) {
        this.predictor = predictor;
        this.graph = graph;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.topN = topN;
        this.threshold = threshold;
        this.progressTracker = progressTracker;
    }

    @Override
    public LinkPredictionResult compute() {
        progressTracker.beginSubTask();
        var result = new LinkPredictionResult(topN);
        var batchQueue = new BatchQueue(graph.nodeCount(), batchSize);
        batchQueue.parallelConsume(concurrency, ignore -> new LinkPredictionScoreByIdsConsumer(
            graph.concurrentCopy(),
            predictor,
            result,
            progressTracker
        ));
        progressTracker.endSubTask();
        return result;
    }

    @Override
    public LinkPredictionPredict me() {
        return this;
    }

    @Override
    public void release() {

    }

    private final class LinkPredictionScoreByIdsConsumer implements Consumer<Batch> {
        private final Graph graph;
        private final LinkLogisticRegressionPredictor predictor;
        private final LinkPredictionResult predictedLinks;
        private final ProgressTracker progressTracker;

        private LinkPredictionScoreByIdsConsumer(
            Graph graph,
            LinkLogisticRegressionPredictor predictor,
            LinkPredictionResult predictedLinks,
            ProgressTracker progressTracker
        ) {
            this.graph = graph;
            this.predictor = predictor;
            this.predictedLinks = predictedLinks;
            this.progressTracker = progressTracker;
        }

        @Override
        public void accept(Batch batch) {
            for (long sourceId : batch.nodeIds()) {
                var neighbors = neighborSet(sourceId);
                // since graph is undirected, only process pairs where sourceId < targetId
                var smallestTarget = sourceId + 1;
                LongStream.range(smallestTarget, graph.nodeCount()).forEach(targetId -> {
                        if (neighbors.contains(targetId)) return;
                        var probability = predictor.predictedProbability(sourceId, targetId);
                        if (probability < threshold) return;
                        predictedLinks.add(sourceId, targetId, probability);
                    }
                );
            }
            progressTracker.logProgress(batch.size());
        }

        private HashSet<Long> neighborSet(long sourceId) {
            var neighbors = new HashSet<Long>();
            graph.forEachRelationship(
                sourceId, (src, trg) -> {
                    neighbors.add(trg);
                    return true;
                }
            );
            return neighbors;
        }
    }
}
