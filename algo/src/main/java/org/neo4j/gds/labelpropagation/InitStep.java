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
package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

final class InitStep implements Step {

    private final NodePropertyValues nodePropertyValues;
    private final HugeLongArray existingLabels;
    private final PrimitiveLongIterable nodes;
    private final Graph graph;
    private final NodePropertyValues nodeWeights;
    private final ProgressTracker progressTracker;
    private final long maxLabelId;

    InitStep(
            Graph graph,
            NodePropertyValues nodePropertyValues,
            NodePropertyValues nodeWeights,
            PrimitiveLongIterable nodes,
            HugeLongArray existingLabels,
            ProgressTracker progressTracker,
            long maxLabelId) {
        this.nodePropertyValues = nodePropertyValues;
        this.existingLabels = existingLabels;
        this.nodes = nodes;
        this.graph = graph;
        this.nodeWeights = nodeWeights;
        this.progressTracker = progressTracker;
        this.maxLabelId = maxLabelId;
    }

    @Override
    public void run() {
        var iterator = nodes.iterator();
        while (iterator.hasNext()) {
            long nodeId = iterator.nextLong();
            long existingLabelValue = nodePropertyValues.longValue(nodeId);
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
            long existingLabel = existingLabelValue == DefaultValue.LONG_DEFAULT_FALLBACK
                    ? maxLabelId + graph.toOriginalNodeId(nodeId) + 1L
                    : existingLabelValue;
            existingLabels.set(nodeId, existingLabel);
            progressTracker.logProgress(graph.degree(nodeId));
        }
    }

    @Override
    public boolean didConverge() {
        return false;
    }

    @Override
    public Step next() {
        return new ComputeStep(
                graph,
                nodeWeights,
                progressTracker,
                existingLabels,
                nodes
        );
    }
}
