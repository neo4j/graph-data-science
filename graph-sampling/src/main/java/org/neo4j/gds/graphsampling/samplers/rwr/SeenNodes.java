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
package org.neo4j.gds.graphsampling.samplers.rwr;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.procedures.LongLongProcedure;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;

import java.util.Arrays;

public interface SeenNodes {

    boolean addNode(long nodeId);

    boolean hasSeenEnough();

    HugeAtomicBitSet sampledNodes();

    long totalExpectedNodes();

    class SeenNodesByLabelSet implements SeenNodes {
        private final Graph inputGraph;
        private final LongLongHashMap seenNodesPerLabelCombination;
        private final LongLongHashMap expectedNodesPerLabelCombination;
        private final NodeLabel[] availableNodeLabels;
        private final HugeAtomicBitSet seenBitSet;
        private final long totalExpectedNodes;

        SeenNodesByLabelSet(Graph inputGraph, NodeLabelHistogram.Result nodeLabelHistogram, double samplingRatio) {
            this.inputGraph = inputGraph;
            this.availableNodeLabels = nodeLabelHistogram.availableNodeLabels();
            this.seenBitSet = HugeAtomicBitSet.create(inputGraph.nodeCount());

            this.expectedNodesPerLabelCombination = new LongLongHashMap(nodeLabelHistogram.histogram().size());
            this.seenNodesPerLabelCombination = new LongLongHashMap(nodeLabelHistogram.histogram().size());
            nodeLabelHistogram.histogram().forEach((LongLongProcedure) (labelCombination, count) -> {
                expectedNodesPerLabelCombination.put(labelCombination, Math.round(samplingRatio * count));
                seenNodesPerLabelCombination.put(labelCombination, 0);
            });
            this.totalExpectedNodes = Arrays.stream(expectedNodesPerLabelCombination.values).sum();
        }

        public boolean addNode(long nodeId) {
            var labelCombination = NodeLabelHistogram.encodedLabelCombination(
                this.inputGraph,
                this.availableNodeLabels,
                nodeId
            );

            // There's a slight race condition here which may cause there to be an extra node or two in a given
            // node label set bucket, since the cardinality check and the set are not synchronized together.
            // Since the sampling is inexact by nature this should be fine.
            if (seenNodesPerLabelCombination.get(labelCombination) < expectedNodesPerLabelCombination.get(
                labelCombination)) {
                if (!seenBitSet.getAndSet(nodeId)) {
                    seenNodesPerLabelCombination.addTo(labelCombination, 1);
                    return true;
                }
            }

            return false;
        }

        public boolean hasSeenEnough() {
            return seenBitSet.cardinality() >= totalExpectedNodes;
        }

        public HugeAtomicBitSet sampledNodes() {
            return seenBitSet;
        }

        @Override
        public long totalExpectedNodes() {
            return totalExpectedNodes;
        }
    }

    class GlobalSeenNodes implements SeenNodes {
        private final HugeAtomicBitSet seenBitSet;
        private final long expectedNodes;

        GlobalSeenNodes(HugeAtomicBitSet seenBitSet, long expectedNodes) {
            this.seenBitSet = seenBitSet;
            this.expectedNodes = expectedNodes;
        }

        public boolean addNode(long nodeId) {
            return !seenBitSet.getAndSet(nodeId);
        }

        public boolean hasSeenEnough() {
            return seenBitSet.cardinality() >= expectedNodes;
        }

        public HugeAtomicBitSet sampledNodes() {
            return seenBitSet;
        }

        @Override
        public long totalExpectedNodes() {
            return expectedNodes;
        }
    }
}
