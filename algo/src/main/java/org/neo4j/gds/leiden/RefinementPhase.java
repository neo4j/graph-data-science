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
package org.neo4j.gds.leiden;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Random;
import java.util.concurrent.atomic.DoubleAdder;

class RefinementPhase {

    private final Graph workingGraph;
    private final HugeLongArray originalCommunities;

    private final HugeDoubleArray nodeVolumes;
    private final HugeDoubleArray communityVolumes;
    private final HugeDoubleArray communityVolumesAfterMerge;
    private final double gamma;
    private final double theta; // randomness

    private final HugeLongArray encounteredCommunities;
    private final HugeDoubleArray encounteredCommunitiesWeights;
    private final long seed;
    private long communityCounter = 0L;

    RefinementPhase(
        Graph workingGraph,
        HugeLongArray originalCommunities,
        HugeDoubleArray nodeVolumes,
        HugeDoubleArray communityVolumes,
        double gamma,
        double theta,
        long seed
    ) {
        this.workingGraph = workingGraph;
        this.originalCommunities = originalCommunities;
        this.nodeVolumes = nodeVolumes;
        this.communityVolumesAfterMerge = nodeVolumes.copyOf(nodeVolumes.size());
        this.communityVolumes = communityVolumes;
        this.gamma = gamma;
        this.theta = theta;

        encounteredCommunities = HugeLongArray.newArray(workingGraph.nodeCount());
        encounteredCommunitiesWeights = HugeDoubleArray.newArray(workingGraph.nodeCount());
        this.seed = seed;
        encounteredCommunitiesWeights.setAll(c -> -1L);
    }

    RefinementPhaseResult run() {
        var refinedCommunities = HugeLongArray.newArray(workingGraph.nodeCount());
        refinedCommunities.setAll(nodeId -> nodeId); //singleton partition

        BitSet singleton = new BitSet(workingGraph.nodeCount());
        singleton.set(0, workingGraph.nodeCount());


        var random = new Random(seed);

        workingGraph.forEachNode(nodeId -> {
            boolean isSingleton = singleton.get(nodeId);
            if (isSingleton && isNodeWellConnected(nodeId)) {
                communityVolumes.set(nodeId, 0);
                mergeNodeSubset(nodeId, refinedCommunities, singleton, random);
            }
            return true;
        });

        return new RefinementPhaseResult(refinedCommunities, communityVolumesAfterMerge);
    }

    private void mergeNodeSubset(
        long nodeId,
        HugeLongArray refinedCommunities,
        BitSet singleton,
        Random random
    ) {

        // Reset the community counter
        communityCounter = 0;

        computeCommunityInformation(nodeId, refinedCommunities);

        var currentNodeCommunityId = refinedCommunities.get(nodeId);
        var currentNodeVolume = nodeVolumes.get(nodeId);

        HugeDoubleArray nextCommunityProbabilities = HugeDoubleArray.newArray(communityCounter);
        long i = 0;
        double probabilitiesSum = 0d;
        if (communityCounter == 0)
            return;

        double bestGain = 0d;
        long bestCommunityId = 0;
        for (long c = 0; c < communityCounter; c++) {
            var candidateCommunityId = encounteredCommunities.get(c);
            var communityRelationshipsCount = encounteredCommunitiesWeights.get(candidateCommunityId);
            encounteredCommunitiesWeights.set(candidateCommunityId, -1);

            var modularityGain =
                communityRelationshipsCount - currentNodeVolume * communityVolumesAfterMerge.get(candidateCommunityId) * gamma;
            if (modularityGain > bestGain) {
                bestGain = modularityGain;
                bestCommunityId = candidateCommunityId;
            }
            double nextCommunityProbability = 0d;
            if (modularityGain >= 0) {
                nextCommunityProbability = Math.exp(modularityGain / theta);
            }

            nextCommunityProbabilities.set(i++, nextCommunityProbability);
            probabilitiesSum += nextCommunityProbability;
        }

        long nextCommunityId = currentNodeCommunityId;

        if (Double.isInfinite(probabilitiesSum) || probabilitiesSum <= 0) {
            if (bestGain > 0) {
                nextCommunityId = bestCommunityId;
            }
        } else {
            var x = probabilitiesSum * random.nextDouble();

            assert x >= 0;

            long j = 0;
            double curr = 0d;
            for (long c = 0; c < communityCounter; c++) {
                var candidateCommunityId = encounteredCommunities.get(c);

                var candidateCommunityProbability = nextCommunityProbabilities.get(j);
                curr += candidateCommunityProbability;

                if (x <= curr) {
                    nextCommunityId = candidateCommunityId;
                    break;
                }

                j++;
            }
        }

        if (nextCommunityId != currentNodeCommunityId) {

            refinedCommunities.set(nodeId, nextCommunityId);
            if (singleton.get(nextCommunityId)) {
                singleton.flip(nextCommunityId);
            }

            var nodeVolume = nodeVolumes.get(nodeId);
            communityVolumesAfterMerge.addTo(nextCommunityId, nodeVolume);
            communityVolumesAfterMerge.addTo(currentNodeCommunityId, -nodeVolume);
        }
    }

    private void computeCommunityInformation(
        long nodeId,
        HugeLongArray refinedCommunities
    ) {
        WellConnectedCommunities wellConnectedCommunities = new WellConnectedCommunities();

        long originalCommunityId = originalCommunities.get(nodeId);
        workingGraph.forEachRelationship(nodeId, 1.0, (s, t, relationshipWeight) -> {
            long tOriginalCommunity = originalCommunities.get(t);
            if (tOriginalCommunity == originalCommunityId) { //they are in the same original partition
                long tCommunity = refinedCommunities.get(t);
                //TODO: Cache WellConnectednessCommunities
                boolean candidateCommunityIsWellConnected = wellConnectedCommunities.test(
                    workingGraph,
                    originalCommunityId,
                    tCommunity,
                    originalCommunities,
                    refinedCommunities,
                    communityVolumes,
                    communityVolumesAfterMerge,
                    gamma
                );

                if (candidateCommunityIsWellConnected) {
                    if (encounteredCommunitiesWeights.get(tCommunity) < 0) {
                        encounteredCommunities.set(communityCounter, tCommunity);
                        communityCounter++;
                        encounteredCommunitiesWeights.set(tCommunity, relationshipWeight);
                    } else {
                        encounteredCommunitiesWeights.addTo(tCommunity, relationshipWeight);
                    }
                }
            }
            return true;
        });
    }

    private boolean isNodeWellConnected(
        long nodeId
    ) {
        var originalCommunityId = originalCommunities.get(nodeId);
        DoubleAdder relationshipsInsideTheCommunity = new DoubleAdder();
        workingGraph.forEachRelationship(nodeId, 1.0, (s, t, relationshipWeight) -> {
            if (s == t) return true;
            long tCommunity = originalCommunities.get(t);
            if (tCommunity == originalCommunityId) relationshipsInsideTheCommunity.add(relationshipWeight);
            return true;
        });
        var externalEdgeVolumePerCommunity = relationshipsInsideTheCommunity.doubleValue();
        var communityVolumeForNode = communityVolumesAfterMerge.get(nodeId);
        var originalCommunityVolume = communityVolumes.get(originalCommunityId);
        return externalEdgeVolumePerCommunity >= communityVolumeForNode * (originalCommunityVolume - communityVolumeForNode) * gamma;

    }
}
