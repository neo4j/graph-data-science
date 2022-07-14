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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;

import java.util.SplittableRandom;

public class RandomWalkWithRestarts implements NodesSampler {

    private final GraphStore inputGraphStore;
    private final RandomWalkWithRestartsConfig config;

    public RandomWalkWithRestarts(
        GraphStore inputGraphStore,
        RandomWalkWithRestartsConfig config
    ) {
        this.inputGraphStore = inputGraphStore;
        this.config = config;
    }

    @Override
    public IdMap sampleNodes(Graph inputGraph) {
        var rng = new SplittableRandom(config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong()));

        boolean hasLabelInformation = !inputGraphStore.nodeLabels().isEmpty();
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(config.concurrency())
            .maxOriginalId(inputGraph.highestNeoId())
            .hasProperties(false)
            .hasLabelInformation(hasLabelInformation)
            .deduplicateIds(false)
            .build();

        long expectedNodes = Math.round(inputGraph.nodeCount() * config.samplingRatio());
        final long startNode = config.startNode().map(inputGraph::toMappedNodeId).orElse(0L);
        var currentNode = new MutableLong(startNode);
        // must keep track of this because nodesBuilder may not have flushed its buffer, so importedNodes cannot be used atm
        var seen = HugeAtomicBitSet.create(inputGraph.nodeCount());
        while (seen.cardinality() < expectedNodes) {
            if (!seen.get(currentNode.getValue())) {
                long originalId = inputGraph.toOriginalNodeId(currentNode.getValue());
                if (hasLabelInformation) {
                    var nodeLabelToken = NodeLabelTokens.of(inputGraph.nodeLabels(currentNode.getValue()));
                    nodesBuilder.addNode(originalId, nodeLabelToken);
                } else {
                    nodesBuilder.addNode(originalId);
                }
                seen.set(currentNode.getValue());
            }
            currentNode.setValue(walkStep(currentNode, startNode, inputGraph, rng));
        }
        var idMapAndProperties = nodesBuilder.build();

        return idMapAndProperties.idMap();
    }

    private long walkStep(MutableLong currentNode, long startNode, Graph inputGraph, SplittableRandom rng) {
        int degree = inputGraph.degree(currentNode.getValue());
        if (degree == 0 || rng.nextDouble() < config.restartProbability()) {
            return startNode;
        }
        int targetOffset = rng.nextInt(degree);

        return inputGraph.getNeighbor(currentNode.getValue(), targetOffset);
    }
}
