/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.labelprop;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

final class InitStep implements Step {

    private final WeightMapping nodeProperties;
    private final HugeLongArray existingLabels;
    private final PrimitiveLongIterable nodes;
    private final Graph graph;
    private final WeightMapping nodeWeights;
    private final ProgressLogger progressLogger;
    private final Direction direction;
    private final long maxLabelId;

    InitStep(
            Graph graph,
            WeightMapping nodeProperties,
            WeightMapping nodeWeights,
            PrimitiveLongIterable nodes,
            HugeLongArray existingLabels,
            ProgressLogger progressLogger,
            Direction direction,
            long maxLabelId) {
        this.nodeProperties = nodeProperties;
        this.existingLabels = existingLabels;
        this.nodes = nodes;
        this.graph = graph;
        this.nodeWeights = nodeWeights;
        this.progressLogger = progressLogger;
        this.direction = direction;
        this.maxLabelId = maxLabelId;
    }

    @Override
    public final void run() {
        PrimitiveLongIterator iterator = nodes.iterator();
        while (iterator.hasNext()) {
            long nodeId = iterator.next();
            double existingLabelValue = nodeProperties.nodeWeight(nodeId, Double.NaN);
            // if there is no provided value for this node, we could start adding
            // to the max provided id and continue from there, but that might
            // clash with node IDs. If we have loaded a graph with a greater node ID
            // than what was provided to us, we would inadvertently put two nodes into
            // the same cluster, that probably have nothing to do with each other.
            // To work around that, we're also adding the node ID to the maxLabelId,
            // basically shifting the node IDs by maxLabelId. We're using the original
            // node ID to maintain determinism since our internal node IDs are not
            // guaranteed to always map in the same fashion to the original IDs and those
            // one are as stable as we need them to be for getting deterministic results.
            long existingLabel = Double.isNaN(existingLabelValue)
                    ? maxLabelId + graph.toOriginalNodeId(nodeId) + 1L
                    : (long) existingLabelValue;
            existingLabels.set(nodeId, existingLabel);
        }
    }

    @Override
    public final Step next() {
        return new ComputeStep(
                graph,
                nodeWeights,
                progressLogger,
                direction,
                existingLabels,
                nodes
        );
    }
}
