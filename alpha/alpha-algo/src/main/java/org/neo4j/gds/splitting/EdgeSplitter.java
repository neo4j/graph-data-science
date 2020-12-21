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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.helpers.collection.Pair;

import java.util.HashSet;
import java.util.Random;

public class EdgeSplitter {

    static final double NEGATIVE = 0D;
    static final double POSITIVE = 1D;

//    private final RelationshipType remainingRelationships;
//    private final RelationshipType selectedRelationships;
//
    private final ThreadLocal<Random> rng;

    public EdgeSplitter(
//        RelationshipType remainingRelationships,
//        RelationshipType selectedRelationships,
        long seed
    ) {
//        this.remainingRelationships = remainingRelationships;
//        this.selectedRelationships = selectedRelationships;
        this.rng = ThreadLocal.withInitial(() -> new Random(seed));
    }

    public SplitResult split(
        Graph graph,
        double removeFraction,
        EdgePredicate allowedNegativeEdges,
        long maxTries,
        long maxTriesNeg
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

        int numPosSamples = (int) (graph.relationshipCount() * removeFraction);
        int numNegSamples = (int) (graph.relationshipCount() * removeFraction);
        var numSamples = numPosSamples + numNegSamples;
        // sample positive examples
        int tries = 0;

        var sampledSet = new HashSet<Pair<Long, Long>>(numSamples);
        while (sampledSet.size() < numPosSamples && tries < maxTries) {
            long source = randomNodeId(graph);
            long targetOffset = randomLong(graph.degree(source));
            long target = targetAtOffset(graph, source, targetOffset);
            Pair<Long, Long> pair = Pair.of(source, target);
            if (sampledSet.add(pair)) {
                selectedRelsBuilder.addFromInternal(source, target, POSITIVE);
            }
            tries++;

        }
        if (tries == maxTries) {
            throw new RuntimeException("Train-Test split exhausted max tries for positive sampling");
        }
        var method = "global";
        // sample negative
        if ("local".equals(method)) {

        } else if ("global".equals(method)) {
            int triesNeg = 0;

            while (sampledSet.size() < numSamples && triesNeg < maxTriesNeg) {
                long source = randomNodeId(graph);
                long target = randomNodeId(graph);
                Pair<Long, Long> pair = Pair.of(source, target);
                if (allowedNegativeEdges.allow(source, target) && sampledSet.add(pair)) {
                    selectedRelsBuilder.addFromInternal(source, target);
                }
                triesNeg++;
            }

        }
        else {
            throw new IllegalArgumentException("Unknown method:" + method);
        }

        RelationshipsBuilder remainingRelsBuilder = GraphFactory.initRelationshipsBuilder()
            .aggregation(Aggregation.SINGLE)
            .nodes(graph.nodeMapping())
            .orientation(Orientation.NATURAL)
            .concurrency(1)
            .executorService(Pools.DEFAULT)
            .tracker(AllocationTracker.empty())
            .build();

        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, (source, target) -> {
                if (!sampledSet.contains(Pair.of(source, target))) {
                    remainingRelsBuilder.addFromInternal(source, target);
                }
                return true;
            });
            return true;
        });

        return SplitResult.of(remainingRelsBuilder.build(), selectedRelsBuilder.build());
    }

    long samplesPerNode(long maxSamples, long remainingSamples, long remainingNodes) {
        var numSamplesOnAverage = ((double) remainingSamples) / remainingNodes;
        return Math.min(Math.min(maxSamples, remainingSamples), Math.round(numSamplesOnAverage));
    }

    private long targetAtOffset(Graph graph, long source, long offset) {
        MutableLong target = new MutableLong();
        MutableLong currentOffset = new MutableLong();
        graph.forEachRelationship(source, (src, trg) -> {
            if (currentOffset.getAndIncrement() == offset) {
                target.setValue(trg);
                return false;
            }
            return true;
        });
        return target.getValue();
    }

    private long randomLong(long bound) {
        return Math.abs(rng.get().nextLong() % bound);
    }

    private long randomNodeId(Graph graph) {
        return randomLong(graph.nodeCount());
    }

    @ValueClass
    interface SplitResult {
        Relationships remainingRels();
        Relationships selectedRels();

        static SplitResult of(Relationships remainingRels, Relationships selectedRels) {
            return ImmutableSplitResult.of(remainingRels, selectedRels);
        }
    }

    interface EdgePredicate {
        boolean allow(long source, long target);
    }

}
