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
import com.carrotsearch.hppc.predicates.LongPredicate;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.BoundedLongLongPriorityQueue;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.linkmodels.ExhaustiveLinkPredictionResult;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureExtractor;

import java.util.Optional;
import java.util.stream.LongStream;

public class ExhaustiveLinkPrediction extends LinkPrediction {
    private final int topN;
    private final double threshold;
    private final TerminationFlag terminationFlag;

    public ExhaustiveLinkPrediction(
        Classifier classifier,
        LinkFeatureExtractor linkFeatureExtractor,
        Graph graph,
        LPNodeFilter sourceNodeFilter,
        LPNodeFilter targetNodeFilter,
        int concurrency,
        int topN,
        double threshold,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(
            classifier,
            linkFeatureExtractor,
            graph,
            sourceNodeFilter,
            targetNodeFilter,
            concurrency,
            progressTracker
        );
        this.topN = topN;
        this.threshold = threshold;
        this.terminationFlag = terminationFlag;
    }

    public static MemoryEstimation estimate(LinkPredictionPredictPipelineBaseConfig config, int linkFeatureDimension) {
        return MemoryEstimations.builder(ExhaustiveLinkPrediction.class.getSimpleName())
            .add("Priority queue", BoundedLongLongPriorityQueue.memoryEstimation(config.topN().orElseThrow()))
            .perGraphDimension("Predict links operation", (dim, threads) -> MemoryRange.of(
                MemoryUsage.sizeOfDoubleArray(linkFeatureDimension) + MemoryUsage.sizeOfLongHashSet(dim.averageDegree())
            ).times(threads))
            .build();
    }

    @Override
    ExhaustiveLinkPredictionResult predictLinks(LinkPredictionSimilarityComputer linkPredictionSimilarityComputer) {
        progressTracker.setSteps(graph.nodeCount());

        var predictionQueue = BoundedLongLongPriorityQueue.max(topN);

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> new LinkPredictionScoreByIdsConsumer(
                graph.concurrentCopy(),
                sourceNodeFilter::test,
                targetNodeFilter::test,
                linkPredictionSimilarityComputer,
                predictionQueue,
                partition,
                progressTracker
            ),
            Optional.of(MIN_NODE_BATCH_SIZE)
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .terminationFlag(terminationFlag)
            .tasks(tasks)
            .run();

        long linksConsidered = tasks.stream().mapToLong(LinkPredictionScoreByIdsConsumer::linksConsidered).sum();
        return new ExhaustiveLinkPredictionResult(predictionQueue, linksConsidered);
    }

    final class LinkPredictionScoreByIdsConsumer implements Runnable {
        private final Graph graph;

        private final LongPredicate sourceNodeFilter;

        private final LongPredicate targetNodeFilter;

        private final LinkPredictionSimilarityComputer linkPredictionSimilarityComputer;
        private final BoundedLongLongPriorityQueue predictionQueue;
        private final ProgressTracker progressTracker;
        private final Partition partition;
        private long linksConsidered;

        LinkPredictionScoreByIdsConsumer(
            Graph graph,
            LongPredicate sourceNodeFilter,
            LongPredicate targetNodeFilter,
            LinkPredictionSimilarityComputer linkPredictionSimilarityComputer,
            BoundedLongLongPriorityQueue predictionQueue,
            Partition partition,
            ProgressTracker progressTracker
        ) {
            this.graph = graph;
            this.sourceNodeFilter = sourceNodeFilter;
            this.targetNodeFilter = targetNodeFilter;
            this.linkPredictionSimilarityComputer = linkPredictionSimilarityComputer;
            this.predictionQueue = predictionQueue;
            this.progressTracker = progressTracker;
            this.partition = partition;
            this.linksConsidered = 0;
        }

        @Override
        public void run() {
            partition.consume(sourceId -> {
                if (sourceNodeFilter.apply(sourceId)) {
                    predictLinksFromNode(sourceId, targetNodeFilter);
                } else if (targetNodeFilter.apply(sourceId)) {
                    predictLinksFromNode(sourceId, sourceNodeFilter);
                }
            });

            progressTracker.logSteps(partition.nodeCount());
        }

        private LongHashSet largerValidNeighbors(long sourceId, LongPredicate targetNodeFilter) {
            var neighbors = new LongHashSet();
            graph.forEachRelationship(
                sourceId, (src, trg) -> {
                    if (src < trg && targetNodeFilter.apply(trg)) neighbors.add(trg);
                    return true;
                }
            );
            return neighbors;
        }

        private void predictLinksFromNode(long sourceId, LongPredicate nodeFilter) {
            var largerNeighbors = largerValidNeighbors(sourceId, nodeFilter);
            // since graph is undirected, only process pairs where sourceId < targetId
            var smallestTarget = sourceId + 1;
            LongStream.range(smallestTarget, graph.nodeCount()).forEach(targetId -> {
                    if (largerNeighbors.contains(targetId)) return;
                    if (nodeFilter.apply(targetId)) {
                        var probability = linkPredictionSimilarityComputer.similarity(sourceId, targetId);
                        linksConsidered++;
                        if (probability < threshold) return;

                        synchronized (predictionQueue) {
                            predictionQueue.offer(sourceId, targetId, probability);
                        }
                    }
                }
            );
        }

        long linksConsidered() {
            return linksConsidered;
        }
    }
}
