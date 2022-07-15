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
package org.neo4j.gds.graphsampling.samplers;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.DoubleCursor;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.SplittableRandom;

public class RandomWalkWithRestarts implements NodesSampler {
    private static final double ALPHA = 0.9;
    private static final double QUALITY_THRESHOLD = 0.05;

    private final GraphStore inputGraphStore;
    private final RandomWalkWithRestartsConfig config;
    private final SplittableRandom rng;

    public RandomWalkWithRestarts(
        GraphStore inputGraphStore,
        RandomWalkWithRestartsConfig config
    ) {
        this.inputGraphStore = inputGraphStore;
        this.config = config;
        this.rng = new SplittableRandom(config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong()));

    }

    @Override
    public IdMap sampleNodes(Graph inputGraph) {

        boolean hasLabelInformation = !inputGraphStore.nodeLabels().isEmpty();
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(config.concurrency())
            .maxOriginalId(inputGraph.highestNeoId())
            .hasProperties(false)
            .hasLabelInformation(hasLabelInformation)
            .deduplicateIds(false)
            .build();

        long expectedNodes = Math.round(inputGraph.nodeCount() * config.samplingRatio());
        LongDoubleMap walkQualityPerStartNode = initializeQualityMap(inputGraph);
        long currentNode = select(walkQualityPerStartNode);
        long currentStartNode = currentNode;
        int addedNodes = 0;
        int walkLength = 1;

        var seen = HugeAtomicBitSet.create(inputGraph.nodeCount());
        while (seen.cardinality() < expectedNodes) {
            if (!seen.get(currentNode)) {
                long originalId = inputGraph.toOriginalNodeId(currentNode);
                if (hasLabelInformation) {
                    var nodeLabelToken = NodeLabelTokens.of(inputGraph.nodeLabels(currentNode));
                    nodesBuilder.addNode(originalId, nodeLabelToken);
                } else {
                    nodesBuilder.addNode(originalId);
                }
                seen.set(currentNode);
                addedNodes++;
            }

            // walk a step
            int degree = inputGraph.degree(currentNode);
            if (degree == 0 || rng.nextDouble() < config.restartProbability()) {
                // walk ended, so check if we need to add a new startNode
                double walkQuality = ((double) addedNodes) / walkLength;
                double oldQuality = walkQualityPerStartNode.get(currentStartNode);
                walkQualityPerStartNode.put(currentStartNode, ALPHA * oldQuality + (1 - ALPHA) * walkQuality);

                double expectedQuality = expectedQuality(walkQualityPerStartNode);
                if (expectedQuality < QUALITY_THRESHOLD) {
                    long newNode = rng.nextLong(inputGraph.nodeCount());
                    walkQualityPerStartNode.put(newNode, 1.0);
                }

                currentStartNode = select(walkQualityPerStartNode);
                currentNode = currentStartNode;
                addedNodes = 0;
                walkLength = 1;
            } else {
                int targetOffset = rng.nextInt(degree);
                currentNode = inputGraph.getNeighbor(currentNode, targetOffset);
                walkLength++;
            }

        }
        var idMapAndProperties = nodesBuilder.build();

        return idMapAndProperties.idMap();
    }

    private double expectedQuality(LongDoubleMap walkQualityPerStartNode) {
        double sumOfQualities = valueSum(walkQualityPerStartNode);
        double sumOfSquaredQualities = 0.0;
        for (DoubleCursor quality : walkQualityPerStartNode.values()) {
            sumOfSquaredQualities += quality.value * quality.value;
        }
        return sumOfSquaredQualities / sumOfQualities;
    }

    private LongDoubleMap initializeQualityMap(Graph inputGraph) {
        var qualityMap = new LongDoubleHashMap();
        if (!config.startNodes().isEmpty()) {
            config.startNodes().forEach(nodeId -> {
                qualityMap.put(inputGraph.toMappedNodeId(nodeId), 1.0);
            });
        } else {
            qualityMap.put(rng.nextLong(inputGraph.nodeCount()), 1.0);
        }
        return qualityMap;
    }

    private long select(LongDoubleMap qualityMap) {
        double sum = valueSum(qualityMap);
        double sample = rng.nextDouble(sum);
        double traversedSum = 0.0;
        for (LongDoubleCursor cursor : qualityMap) {
            traversedSum += cursor.value;
            if (traversedSum >= sample) {
                return cursor.key;
            }
        }
        throw new IllegalStateException("Something went wrong :(");
    }

    private double valueSum(LongDoubleMap qualityMap) {
        double sum = 0.0;
        for (DoubleCursor quality : qualityMap.values()) {
            sum += quality.value;
        }
        return sum;
    }

}
