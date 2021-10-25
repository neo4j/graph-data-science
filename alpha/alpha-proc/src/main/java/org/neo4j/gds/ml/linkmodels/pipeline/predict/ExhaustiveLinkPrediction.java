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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.linkmodels.ExhaustiveLinkPredictionResult;
import org.neo4j.gds.ml.linkmodels.pipeline.PipelineExecutor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.LongStream;

public class ExhaustiveLinkPrediction extends LinkPrediction {
    private final int topN;
    private final double threshold;

    public ExhaustiveLinkPrediction(
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
        super(
            modelData,
            pipelineExecutor,
            nodeLabels,
            relationshipTypes,
            graphStore,
            concurrency,
            progressTracker
        );
        this.topN = topN;
        this.threshold = threshold;
    }

    @Override
    ExhaustiveLinkPredictionResult predictLinks(
        Graph graph,
        LinkPredictionSimilarityComputer linkPredictionSimilarityComputer
    ) {
        var result = new ExhaustiveLinkPredictionResult(topN);

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> new LinkPredictionScoreByIdsConsumer(
                graph.concurrentCopy(),
                linkPredictionSimilarityComputer,
                result,
                partition,
                progressTracker
            ),
            Optional.empty()
        );

        ParallelUtil.runWithConcurrency(concurrency, tasks, Pools.DEFAULT);

        result.setLinksConsidered(tasks.stream().mapToLong(LinkPredictionScoreByIdsConsumer::linksConsidered).sum());

        return result;
    }

    final class LinkPredictionScoreByIdsConsumer implements Runnable {
        private final Graph graph;
        private final LinkPredictionSimilarityComputer linkPredictionSimilarityComputer;
        private final ExhaustiveLinkPredictionResult predictedLinks;
        private final ProgressTracker progressTracker;
        private final Partition partition;
        private long linksConsidered;

        LinkPredictionScoreByIdsConsumer(
            Graph graph,
            LinkPredictionSimilarityComputer linkPredictionSimilarityComputer,
            ExhaustiveLinkPredictionResult predictedLinks,
            Partition partition,
            ProgressTracker progressTracker
        ) {
            this.graph = graph;
            this.linkPredictionSimilarityComputer = linkPredictionSimilarityComputer;
            this.predictedLinks = predictedLinks;
            this.progressTracker = progressTracker;
            this.partition = partition;
            this.linksConsidered = 0;
        }

        @Override
        public void run() {
            partition.consume(sourceId -> {
                var largerNeighbors = largerNeighbors(sourceId);
                // since graph is undirected, only process pairs where sourceId < targetId
                var smallestTarget = sourceId + 1;
                LongStream.range(smallestTarget, graph.nodeCount()).forEach(targetId -> {
                        if (largerNeighbors.contains(targetId)) return;
                        var probability = linkPredictionSimilarityComputer.similarity(sourceId, targetId);
                        linksConsidered++;
                        if (probability < threshold) return;
                        predictedLinks.add(sourceId, targetId, probability);
                    }
                );
            });

            progressTracker.logProgress(partition.nodeCount());
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

        long linksConsidered() {
            return linksConsidered;
        }
    }
}
