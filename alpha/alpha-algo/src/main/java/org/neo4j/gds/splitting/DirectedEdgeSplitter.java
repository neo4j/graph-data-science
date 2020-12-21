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
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

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

        RelationshipsBuilder selectedRelsBuilder = GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(graph.nodeMapping())
            .orientation(Orientation.NATURAL)
            .loadRelationshipProperty(true)
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .tracker(AllocationTracker.empty())
            .build();

        RelationshipsBuilder remainingRelsBuilder = GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(graph.nodeMapping())
            .orientation(Orientation.NATURAL)
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .tracker(AllocationTracker.empty())
            .build();


        int numPosSamples = (int) (graph.relationshipCount() * holdoutFraction);
        int numNegSamples = (int) (graph.relationshipCount() * holdoutFraction);

        var selectedPositiveCount = new AtomicLong(0L);
        var selectedNegativeCount = new AtomicLong(0L);
        graph.forEachNode(nodeId -> {
            var degree = graph.degree(nodeId);

            var posEdges = samplesPerNode(
                degree,
                numPosSamples - selectedPositiveCount.get(),
                graph.nodeCount() - nodeId
            );
            var preSelectedCount = selectedPositiveCount.get();

            var targetsRemaining = new AtomicLong(degree);

            graph.forEachRelationship(nodeId, (source, target) -> {
                var localSelectedCount = selectedPositiveCount.get() - preSelectedCount;
                double localSelectedRemaining = posEdges - localSelectedCount;
                var pickThisOne = sample(localSelectedRemaining / targetsRemaining.getAndDecrement());
                if (posEdges > 0 && localSelectedRemaining > 0 && pickThisOne) {
                    selectedPositiveCount.incrementAndGet();
                    selectedRelsBuilder.addFromInternal(source, target, POSITIVE);
                } else {
                    remainingRelsBuilder.addFromInternal(source, target);
                }
                return true;
            });

            var masterDegree = masterGraph.degree(nodeId);
            var possibleNegEdges = (masterGraph.nodeCount() - 1) - masterDegree;
            var negEdges = samplesPerNode(
                possibleNegEdges,
                numNegSamples - selectedNegativeCount.get(),
                graph.nodeCount() - nodeId
            );

            var neighbours = new HashSet<Long>(masterDegree);
            masterGraph.forEachRelationship(nodeId, (source, target) -> {
                neighbours.add(target);
                return true;
            });

            for (int i = 0; i < negEdges; i++) {
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
