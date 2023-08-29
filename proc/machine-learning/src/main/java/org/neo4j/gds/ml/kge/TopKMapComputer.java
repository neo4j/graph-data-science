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
package org.neo4j.gds.ml.kge;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.predicates.LongLongPredicate;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.SetBitsIterable;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.nodesim.TopKMap;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;

import java.util.stream.LongStream;

public class TopKMapComputer {

    private ProgressTracker progressTracker;
    private BitSet sourceNodes;
    private BitSet targetNodes;
    private int concurrency;

    private int topK;
    private TerminationFlag terminationFlag;
    private LinkScorerFactory linkScorerFactory;

    // TODO abstract / merge with LinkFilter?
    private LongLongPredicate isInvalidLink;

    public TopKMapComputer(
        BitSet sourceNodes,
        BitSet targetNodes,
        LinkScorerFactory linkScorerFactory,
        LongLongPredicate isInvalidLink,
        int topK,
        int concurrency,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.progressTracker = progressTracker;
        this.sourceNodes = sourceNodes;
        this.targetNodes = targetNodes;
        this.concurrency = concurrency;
        this.topK = topK;
        this.terminationFlag = terminationFlag;
        this.linkScorerFactory = linkScorerFactory;
        this.isInvalidLink = isInvalidLink;
    }

    public TopKMap compute() {
        progressTracker.beginSubTask(estimateWorkload());

        TopKMap topKMap = new TopKMap(sourceNodes.capacity(), sourceNodes, Math.abs(topK), true);

        try (var threadLocalSimilarityComputer = AutoCloseableThreadLocal.withInitial(linkScorerFactory::create)) {
            // TODO exploit symmetry of similarity function if available
            ParallelUtil.parallelStreamConsume(
                new SetBitsIterable(sourceNodes).stream(),
                concurrency,
                terminationFlag,
                stream -> stream
                    .forEach(node1 -> {
                        terminationFlag.assertRunning();

                        LinkScorer similarityComputer = threadLocalSimilarityComputer.get();
                        similarityComputer.init(node1);

                        targetNodesStream()
                            .filter(node2 -> isInvalidLink.apply(node1, node2))
                            .forEach(node2 -> {
                                double similarity = similarityComputer.similarity(node2);
                                if (!Double.isNaN(similarity)) {
                                    topKMap.put(node1, node2, similarity);
                                }
                                progressTracker.logProgress();
                            });
                    })
            );
        }

        progressTracker.endSubTask();

        return topKMap;
    }


    private LongStream targetNodesStream() {
        return new SetBitsIterable(targetNodes, 0).stream();
    }

    private long estimateWorkload() {
        return sourceNodes.cardinality() * targetNodes.cardinality();
    }
}
