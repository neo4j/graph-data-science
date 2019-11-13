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

package org.neo4j.graphalgo.impl.modularity;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

final class ModularityOptimizationTask implements Runnable {

    private final Graph graph;
    private final RelationshipIterator localGraph;
    private final long batchStart;
    private final long batchEnd;
    private final long color;
    private final double totalNodeWeight;
    private final Direction direction;
    private final HugeLongArray colors;
    private final HugeLongArray currentCommunities;
    private final HugeLongArray nextCommunities;
    private final HugeDoubleArray cumulativeNodeWeights;
    private final HugeDoubleArray nodeCommunityInfluences;
    private final HugeAtomicDoubleArray communityWeights;
    private final HugeAtomicDoubleArray communityWeightUpdates;

    ModularityOptimizationTask(
        Graph graph,
        long batchStart,
        long batchEnd,
        long color,
        double totalNodeWeight,
        Direction direction,
        HugeLongArray colors,
        HugeLongArray currentCommunities,
        HugeLongArray nextCommunities,
        HugeDoubleArray cumulativeNodeWeights,
        HugeDoubleArray nodeCommunityInfluences,
        HugeAtomicDoubleArray communityWeights,
        HugeAtomicDoubleArray communityWeightUpdates
    ) {
        this.graph = graph;
        this.batchStart = batchStart;
        this.batchEnd = batchEnd;
        this.color = color;
        this.localGraph = graph.concurrentCopy();
        this.currentCommunities = currentCommunities;
        this.nextCommunities = nextCommunities;
        this.communityWeights = communityWeights;
        this.communityWeightUpdates = communityWeightUpdates;
        this.totalNodeWeight = totalNodeWeight;
        this.cumulativeNodeWeights = cumulativeNodeWeights;
        this.nodeCommunityInfluences = nodeCommunityInfluences;
        this.colors = colors;
        this.direction = direction;
    }

    @Override
    public void run() {
        for (long nodeId = batchStart; nodeId < batchEnd; nodeId++) {

            if (colors.get(nodeId) != color) {
                continue;
            }

            long currentCommunity = currentCommunities.get(nodeId);
            final int degree = graph.degree(nodeId, direction);
            LongDoubleMap communityInfluences = new LongDoubleHashMap(degree);
            MutableDouble selfWeight = new MutableDouble(0.0D);

            // calculate influence of this node w.r.t its neighbours communities
            localGraph.forEachRelationship(nodeId, direction, 1.0D, (s, t, w) -> {
                if (s == t) {
                    selfWeight.add(w);
                } else {
                    long targetCommunity = currentCommunities.get(t);
                    communityInfluences.addTo(targetCommunity, w);
                }
                return true;
            });

            long nextCommunity = currentCommunity;
            double currentGain;
            double maxGain = 0.0;
            double eix = communityInfluences.get(currentCommunity) - selfWeight.doubleValue();
            double cumulativeNodeWeight = cumulativeNodeWeights.get(nodeId);
            double ax = communityWeights.get(currentCommunity) - cumulativeNodeWeight;
            double eiy;
            double ay;

            long communityCandidate;
            for (LongDoubleCursor cursor : communityInfluences) {
                communityCandidate = cursor.key;

                if (currentCommunity != communityCandidate) {
                    ay = communityWeights.get(communityCandidate);
                    eiy = cursor.value;
                    currentGain =
                        (eiy - eix) / totalNodeWeight
                        + (2 * cumulativeNodeWeight * ax - 2 * cumulativeNodeWeight * ay) / Math.pow(
                            2 * totalNodeWeight,
                            2
                        );

                    if ((currentGain > maxGain) || (currentGain == maxGain && currentGain != 0.0 && nextCommunity > communityCandidate)) {
                        maxGain = currentGain;
                        nextCommunity = communityCandidate;
                    }
                }
            }

            nodeCommunityInfluences.set(nodeId, communityInfluences.get(nextCommunity));

            // TODO implement swap protection
            nextCommunities.set(nodeId, nextCommunity);
            communityWeightUpdates.update(currentCommunity, agg -> agg - cumulativeNodeWeight);
            communityWeightUpdates.update(nextCommunity, agg -> agg + cumulativeNodeWeight);
        }

    }
}
