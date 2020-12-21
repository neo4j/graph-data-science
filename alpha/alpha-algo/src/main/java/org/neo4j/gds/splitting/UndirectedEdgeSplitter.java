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


/**
 * Splits an undirected graph into two Relationships objects.
 * The first represents a holdout set and is a directed graph.
 * The second represents the remaining graph and is also undirected.
 * For each held out undirected edge, the holdout set is populated with
 * an edge with the same underlying node pair but with random direction.
 * The holdout fraction is denominated in fraction of undirected edges.
 */
public class UndirectedEdgeSplitter extends EdgeSplitterBase {
    UndirectedEdgeSplitter(long seed) {
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
        // TODO: move this validation higher into the hierarchy
        if (!graph.isUndirected()) {
            throw new IllegalArgumentException("EdgeSplitter requires graph to be UNDIRECTED");
        }
        if (!masterGraph.isUndirected()) {
            throw new IllegalArgumentException("EdgeSplitter requires master graph to be UNDIRECTED");
        }

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
            .orientation(Orientation.UNDIRECTED)
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .tracker(AllocationTracker.empty())
            .build();

        var posSamplesRemaining = new AtomicLong((long) (graph.relationshipCount() * holdoutFraction) / 2);
        var negSamplesRemaining = new AtomicLong((long) (graph.relationshipCount() * holdoutFraction) / 2);
        var edgesRemaining = new AtomicLong(graph.relationshipCount());

        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, (source, target) -> {
                if (source == target) {
                    edgesRemaining.decrementAndGet();
                }
                if (source < target) {
                    // we handle also reverse edge here
                    // the effect of self-loops are disregarded
                    if (sample((double) 2 * posSamplesRemaining.get() / edgesRemaining.get())) {
                        posSamplesRemaining.decrementAndGet();
                        if (sample(0.5)) {
                            selectedRelsBuilder.addFromInternal(source, target, POSITIVE);
                        } else {
                            selectedRelsBuilder.addFromInternal(target, source, POSITIVE);
                        }
                    } else {
                        remainingRelsBuilder.addFromInternal(source, target);
                    }
                    // because of reverse edge
                    edgesRemaining.addAndGet(-2);
                }
                return true;
            });

            var masterDegree = masterGraph.degree(nodeId);
            var possibleNegEdges = (masterGraph.nodeCount() - 1) - masterDegree;
            var negEdges = samplesPerNode(
                possibleNegEdges,
                (double) negSamplesRemaining.get(),
                graph.nodeCount() - nodeId
            );

            var neighbours = new HashSet<Long>(masterDegree);
            masterGraph.forEachRelationship(nodeId, (source, target) -> {
                neighbours.add(target);
                return true;
            });

            // this will not try to avoid duplicate negative relationships,
            // nor will it avoid sampling edges that are sampled as negative in
            // an outer split.
            for (int i = 0; i < negEdges; i++) {
                var negativeTarget = randomNodeId(graph);
                // no self-relationships
                if (!neighbours.contains(negativeTarget) && negativeTarget != nodeId) {
                    negSamplesRemaining.decrementAndGet();
                    selectedRelsBuilder.addFromInternal(nodeId, negativeTarget, NEGATIVE);
                }
            }
            return true;
        });

        return SplitResult.of(remainingRelsBuilder.build(), selectedRelsBuilder.build());
    }
}
