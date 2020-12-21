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
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class EdgeSplitter {

    static final double NEGATIVE = 0D;
    static final double POSITIVE = 1D;

    private final ThreadLocal<Random> rng;

    public EdgeSplitter(
        long seed
    ) {
        this.rng = ThreadLocal.withInitial(() -> new Random(seed));
    }

    public SplitResult split(
        Graph graph,
        double removeFraction
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


        int numPosSamples = (int) (graph.relationshipCount() * removeFraction);
        int numNegSamples = (int) (graph.relationshipCount() * removeFraction);

        var selectedPositiveCount = new AtomicLong(0L);
        var selectedNegativeCount = new AtomicLong(0L);
        graph.forEachNode(nodeId -> {
            var degree = graph.degree(nodeId);
            var neighbours = new HashSet<Long>(degree);

            var sampledPosEdges = samplesPerNode(
                degree,
                numPosSamples - selectedPositiveCount.get(),
                graph.nodeCount() - nodeId
            );
            var preSelectedCount = selectedPositiveCount.get();

            graph.forEachRelationship(nodeId, (source, target) -> {
                neighbours.add(target);
                var localSelectedCount = selectedPositiveCount.get() - preSelectedCount;
                double localSelectedRemaining = sampledPosEdges - localSelectedCount;
                var pickThisOne = sample(localSelectedRemaining / sampledPosEdges);
                if (sampledPosEdges > 0 && localSelectedRemaining > 0 && pickThisOne) {
                    selectedPositiveCount.incrementAndGet();
                    selectedRelsBuilder.addFromInternal(source, target, POSITIVE);
                } else {
                    remainingRelsBuilder.addFromInternal(source, target);
                }
                return true;
            });

            var possibleNegEdges = (graph.nodeCount() - 1) - degree;
            var sampledNegEdges = samplesPerNode(
                possibleNegEdges,
                numNegSamples - selectedNegativeCount.get(),
                graph.nodeCount() - nodeId
            );
            for (int i = 0; i < sampledNegEdges; i++) {
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

    long samplesPerNode(long maxSamples, long remainingSamples, long remainingNodes) {
        var numSamplesOnAverage = ((double) remainingSamples) / remainingNodes;
        return Math.min(maxSamples, Math.round(numSamplesOnAverage));
    }

    private boolean sample(double probability) {
        return rng.get().nextDouble() < probability;
    }

    private long randomNodeId(Graph graph) {
        return Math.abs(rng.get().nextLong() % graph.nodeCount());
    }

    @ValueClass
    interface SplitResult {
        Relationships remainingRels();
        Relationships selectedRels();

        static SplitResult of(Relationships remainingRels, Relationships selectedRels) {
            return ImmutableSplitResult.of(remainingRels, selectedRels);
        }
    }
}
