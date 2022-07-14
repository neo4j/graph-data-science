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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

public class RandomWalkWithRestarts implements NodesSampler {

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
        final List<Long> startNodes = getStartNodes(inputGraph);
        var currentNode = startNodes.get(rng.nextInt(startNodes.size()));
        // must keep track of this because nodesBuilder may not have flushed its buffer, so importedNodes cannot be used atm
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
            }
            currentNode  = walkStep(currentNode, startNodes, inputGraph, rng);
        }
        var idMapAndProperties = nodesBuilder.build();

        return idMapAndProperties.idMap();
    }

    private List<Long> getStartNodes(Graph inputGraph) {
        if (!config.startNodes().isEmpty()) {
            return config.startNodes().stream().map(inputGraph::toMappedNodeId).collect(Collectors.toList());
        }
        return List.of(rng.nextLong(inputGraph.nodeCount()));
    }

    private long walkStep(long currentNode, List<Long> startNodes, Graph inputGraph, SplittableRandom rng) {
        int degree = inputGraph.degree(currentNode);
        if (degree == 0 || rng.nextDouble() < config.restartProbability()) {
            return startNodes.get(rng.nextInt(startNodes.size()));
        }
        int targetOffset = rng.nextInt(degree);

        return inputGraph.getNeighbor(currentNode, targetOffset);
    }
}
