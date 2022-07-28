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
package org.neo4j.gds.beta.modularity;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

final class ModularityOptimizationTask implements Runnable {

    private final Graph localGraph;
    private final Partition partition;
    private final long color;
    private final double totalNodeWeight;
    private final HugeLongArray colors;
    private final ProgressTracker progressTracker;
    private final HugeLongArray currentCommunities;
    private final HugeLongArray nextCommunities;
    private final HugeDoubleArray cumulativeNodeWeights;
    private final ModularityManager modularityManager;
    private final HugeAtomicDoubleArray communityWeightUpdates;

    ModularityOptimizationTask(
        Graph graph,
        Partition partition,
        long color,
        double totalNodeWeight,
        HugeLongArray colors,
        HugeLongArray currentCommunities,
        HugeLongArray nextCommunities,
        HugeDoubleArray cumulativeNodeWeights,
        HugeAtomicDoubleArray communityWeightUpdates,
        ModularityManager modularityManager,
        ProgressTracker progressTracker
    ) {
        this.partition = partition;
        this.color = color;
        this.localGraph = graph.concurrentCopy();
        this.currentCommunities = currentCommunities;
        this.nextCommunities = nextCommunities;
        this.modularityManager = modularityManager;
        this.communityWeightUpdates = communityWeightUpdates;
        this.totalNodeWeight = totalNodeWeight;
        this.cumulativeNodeWeights = cumulativeNodeWeights;
        this.colors = colors;
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        LongDoubleMap reuseCommunityInfluences = new LongDoubleHashMap(50);
        var relationshipsProcessed = new MutableLong();

        partition.consume(nodeId -> {
            if (colors.get(nodeId) != color) {
                return;
            }

            long currentCommunity = currentCommunities.get(nodeId);
            final int degree = localGraph.degree(nodeId);

            LongDoubleMap communityInfluences;
            if (degree < 50) {
                reuseCommunityInfluences.clear();
                communityInfluences = reuseCommunityInfluences;
            } else {
                communityInfluences = new LongDoubleHashMap(degree);
            }
            MutableDouble selfWeight = new MutableDouble(0.0D);

            // calculate influence of this node w.r.t its neighbours communities
            localGraph.forEachRelationship(nodeId, 1.0D, (s, t, w) -> {
                if (s == t) {
                    selfWeight.add(w);
                }
                long targetCommunity = currentCommunities.get(t);
                communityInfluences.addTo(targetCommunity, w);
                return true;
            });

            long nextCommunity = currentCommunity;
            double currentGain;
            double maxGain = 0.0;
            double eix = communityInfluences.get(currentCommunity) - selfWeight.doubleValue();
            double cumulativeNodeWeight = cumulativeNodeWeights.get(nodeId);
            double ax = modularityManager.getCommunityWeight(currentCommunity) - cumulativeNodeWeight;
            double eiy;
            double ay;

            long communityCandidate;
            for (LongDoubleCursor cursor : communityInfluences) {
                communityCandidate = cursor.key;

                if (currentCommunity != communityCandidate) {
                    ay = modularityManager.getCommunityWeight(communityCandidate);
                    eiy = cursor.value;
                    currentGain =
                        (eiy - eix) / (totalNodeWeight / 2.0)
                        + (2 * cumulativeNodeWeight * ax - 2 * cumulativeNodeWeight * ay) / Math.pow(
                            totalNodeWeight,
                            2
                        );

                    if ((currentGain > maxGain) || (currentGain == maxGain && currentGain != 0.0 && nextCommunity > communityCandidate)) {
                        maxGain = currentGain;
                        nextCommunity = communityCandidate;
                    }
                }
            }


            nextCommunities.set(nodeId, nextCommunity);
            communityWeightUpdates.update(currentCommunity, agg -> agg - cumulativeNodeWeight);
            communityWeightUpdates.update(nextCommunity, agg -> agg + cumulativeNodeWeight);

            relationshipsProcessed.add(degree);
        });

        progressTracker.logProgress(relationshipsProcessed.longValue());
    }
}
