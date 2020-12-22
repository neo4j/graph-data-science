/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.splitting;

import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

public class DirectedEdgeSplitter extends EdgeSplitterBase {

    public DirectedEdgeSplitter(long seed) {
        super(seed);
    }

    public SplitResult split(
        Graph graph,
        double holdoutFraction
    ) {
        return split(graph, graph, holdoutFraction);
    }

    public SplitResult split(
        Graph graph,
        Graph masterGraph,
        double holdoutFraction
    ) {

        RelationshipsBuilder selectedRelsBuilder = newRelationshipsBuilderWithProp(graph, Orientation.NATURAL);

        RelationshipsBuilder remainingRelsBuilder = newRelationshipsBuilder(graph, Orientation.NATURAL);

        int totalPositiveSamples = (int) (graph.relationshipCount() * holdoutFraction);
        int totalNegativeSamples = (int) (graph.relationshipCount() * holdoutFraction);

        var selectedPositiveCount = new AtomicLong(0L);
        var selectedNegativeCount = new AtomicLong(0L);
        graph.forEachNode(nodeId -> {
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

            var masterDegree = masterGraph.degree(nodeId);
            var negativeEdgeCount = samplesPerNode(
                (masterGraph.nodeCount() - 1) - masterDegree,
                totalNegativeSamples - selectedNegativeCount.get(),
                graph.nodeCount() - nodeId
            );

            var neighbours = new HashSet<Long>(masterDegree);
            masterGraph.forEachRelationship(nodeId, (source, target) -> {
                neighbours.add(target);
                return true;
            });

            for (int i = 0; i < negativeEdgeCount; i++) {
                var negativeTarget = randomNodeId(graph);
                // no self-relationships
                if (!neighbours.contains(negativeTarget) && negativeTarget != nodeId) {
                    selectedNegativeCount.incrementAndGet();
                    selectedRelsBuilder.addFromInternal(nodeId, negativeTarget, NEGATIVE);
                }
            }
            return true;
        });

        return SplitResult.of(remainingRelsBuilder.build(), selectedRelsBuilder.build());
    }
}
