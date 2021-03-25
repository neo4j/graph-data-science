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
package org.neo4j.gds.ml.splitting;

import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class DirectedEdgeSplitter extends EdgeSplitter {

    public DirectedEdgeSplitter(Optional<Long> maybeSeed) {
        super(maybeSeed);
    }

    public DirectedEdgeSplitter(long seed) {
        this(Optional.of(seed));
    }

    public DirectedEdgeSplitter(Optional<Long> maybeSeed, int negativeRatio) {
        super(maybeSeed, negativeRatio);
    }

    @Override
    public SplitResult split(
        Graph graph,
        double holdoutFraction
    ) {
        return split(graph, graph, holdoutFraction);
    }

    @Override
    public SplitResult split(
        Graph graph,
        Graph masterGraph,
        double holdoutFraction
    ) {

        RelationshipsBuilder selectedRelsBuilder = newRelationshipsBuilderWithProp(graph, Orientation.NATURAL);

        RelationshipsBuilder remainingRelsBuilder = newRelationshipsBuilder(graph, Orientation.NATURAL);

        int totalPositiveSamples = (int) (graph.relationshipCount() * holdoutFraction);
        var negativeSamplesRemaining = new AtomicLong((long) (negativeRatio * graph.relationshipCount() * holdoutFraction));

        var selectedPositiveCount = new AtomicLong(0L);
        graph.forEachNode(nodeId -> {
            positiveSampling(
                graph,
                selectedRelsBuilder,
                remainingRelsBuilder,
                totalPositiveSamples,
                selectedPositiveCount,
                nodeId
            );

            negativeSampling(
                graph,
                masterGraph,
                selectedRelsBuilder,
                negativeSamplesRemaining,
                nodeId
            );
            return true;
        });

        return SplitResult.of(remainingRelsBuilder.build(), selectedRelsBuilder.build());
    }

    private void positiveSampling(
        Graph graph,
        RelationshipsBuilder selectedRelsBuilder,
        RelationshipsBuilder remainingRelsBuilder,
        int totalPositiveSamples,
        AtomicLong selectedPositiveCount,
        long nodeId
    ) {
        var degree = graph.degree(nodeId);

        var positiveEdgeCount = samplesPerNode(
            degree,
            totalPositiveSamples - selectedPositiveCount.get(),
            graph.nodeCount() - nodeId
        );
        var preSelectedCount = selectedPositiveCount.get();

        var targetsRemaining = new AtomicLong(degree);

        graph.forEachRelationship(nodeId, (source, target) -> {
            var localSelectedCount = selectedPositiveCount.get() - preSelectedCount;
            double localSelectedRemaining = positiveEdgeCount - localSelectedCount;
            var isSelected = sample(localSelectedRemaining / targetsRemaining.getAndDecrement());
            if (positiveEdgeCount > 0 && localSelectedRemaining > 0 && isSelected) {
                selectedPositiveCount.incrementAndGet();
                selectedRelsBuilder.addFromInternal(source, target, POSITIVE);
            } else {
                remainingRelsBuilder.addFromInternal(source, target);
            }
            return true;
        });
    }
}
