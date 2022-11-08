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
package org.neo4j.gds.ml.negativeSampling;

import com.carrotsearch.hppc.predicates.LongPredicate;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;

import java.util.HashSet;
import java.util.Optional;
import java.util.SplittableRandom;

// Negative sampling does not guarantee negativeSamplesRemaining number of negative edges are sampled.
// because 1. for dense graphs there aren't enough possible negative edges
// and 2. If the last few nodes are dense, since we calculate negative samples needed per node, there won't be enough negative samples added.
public class RandomNegativeSampler implements NegativeSampler {

    private static final int MAX_RETRIES = 20;

    private final SplittableRandom rng;

    private final Graph graph;
    private final long testSampleCount;
    private final long trainSampleCount;
    private final IdMap validSourceNodes;
    private final IdMap validTargetNodes;

    public RandomNegativeSampler(
        Graph graph,
        long testSampleCount,
        long trainSampleCount,
        IdMap validSourceNodes,
        IdMap validTargetNodes,
        Optional<Long> randomSeed
    ) {
        this.graph = graph;
        this.testSampleCount = testSampleCount;
        this.trainSampleCount = trainSampleCount;
        this.validSourceNodes = validSourceNodes;
        this.validTargetNodes = validTargetNodes;
        this.rng = randomSeed.map(SplittableRandom::new).orElseGet(SplittableRandom::new);
    }

    @Override
    public void produceNegativeSamples(
        RelationshipsBuilder testSetBuilder,
        RelationshipsBuilder trainSetBuilder
    ) {
        var remainingTestSamples = new MutableLong(testSampleCount);
        var remainingTrainSamples = new MutableLong(trainSampleCount);
        var remainingValidSourceNodes = new MutableLong(validSourceNodes.nodeCount());
        LongPredicate isValidSourceNodes = nodeId -> validSourceNodes.contains(graph.toOriginalNodeId(nodeId));
        LongPredicate isValidTargetNodes = nodeId -> validTargetNodes.contains(graph.toOriginalNodeId(nodeId));

        graph.forEachNode(nodeId -> {
            if (!isValidSourceNodes.apply(nodeId)) {
                return true;
            }
            var masterDegree = graph.degree(nodeId);
            var negativeEdgeCount = samplesPerNode(
                (graph.nodeCount() - 1) - masterDegree,
                remainingTestSamples.longValue() + remainingTrainSamples.longValue(),
                remainingValidSourceNodes.getAndDecrement()
            );

            var neighbours = new HashSet<Long>(masterDegree);
            graph.forEachRelationship(nodeId, (source, target) -> {
                neighbours.add(target);
                return true;
            });

            // this will not try to avoid duplicate negative relationships,
            // nor will it avoid sampling edges that are sampled as negative in
            // an outer split.
            int retries = MAX_RETRIES;
            for (int i = 0; i < negativeEdgeCount; i++) {
                var negativeTarget = randomNodeId(graph);
                // no self-relationships
                if (isValidTargetNodes.apply(negativeTarget) && !neighbours.contains(negativeTarget) && negativeTarget != nodeId) {
                    //FIXME: Put in parallel and remove nodeId bias
                    if (remainingTestSamples.longValue() > 0l) {
                        remainingTestSamples.decrement();
                        testSetBuilder.addFromInternal(
                            graph.toRootNodeId(nodeId),
                            graph.toRootNodeId(negativeTarget),
                            NEGATIVE
                        );
                    } else if (remainingTrainSamples.longValue() > 0) {
                        remainingTrainSamples.decrement();
                        trainSetBuilder.addFromInternal(
                            graph.toRootNodeId(nodeId),
                            graph.toRootNodeId(negativeTarget),
                            NEGATIVE
                        );
                    } else {
                        //both testSampleCount = trainSampleCount = 0
                        return false;
                    }
                } else if (retries-- > 0) {
                    // we retry with a different negative target
                    // skipping here and relying on finding another source node is not safe
                    // we only retry a few times to protect against resampling forever for high deg nodes
                    i--;
                }
            }
            return true;
        });
    }

    private long randomNodeId(Graph graph) {
        return Math.abs(rng.nextLong() % graph.nodeCount());
    }

    private long samplesPerNode(long maxSamples, double remainingSamples, long remainingNodes) {
        var numSamplesOnAverage = remainingSamples / remainingNodes;
        var wholeSamples = (long) numSamplesOnAverage;
        var extraSample = sample(numSamplesOnAverage - wholeSamples) ? 1 : 0;
        return Math.min(maxSamples, wholeSamples + extraSample);
    }

    private boolean sample(double probability) {
        return rng.nextDouble() < probability;
    }

}
