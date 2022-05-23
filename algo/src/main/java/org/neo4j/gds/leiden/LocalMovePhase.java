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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

final class LocalMovePhase {

    private final Graph graph;
    private final HugeLongArray currentCommunities;
    //Idx   - nodeId
    //Value
    //       - Unweighted : degree of node
    //       - Weighted   : sum of the relationships weights of the node
    private final HugeDoubleArray nodeVolumes;

    // Idx   - communityId
    // Value
    //       - Unweighted : sum of the degrees of the nodes in the community.
    //       - Weighted   : sum of the relationship weights of the nodes in the community.
    // Note: the values also count relationships to nodes outside the community.
    private final HugeDoubleArray communityVolumes;
    private final double gamma;

    private final HugeLongArray encounteredCommunities;
    private final HugeDoubleArray encounteredCommunitiesWeights;
    private long encounteredCommunityCounter = 0;

    long swaps;

    private long communityCount;

    static LocalMovePhase create(
        Graph graph,
        HugeLongArray seedCommunities,
        HugeDoubleArray nodeVolumes,
        HugeDoubleArray communityVolumes,
        double gamma,
        long communityCount
    ) {

        var encounteredCommunities = HugeLongArray.newArray(graph.nodeCount());
        var encounteredCommunitiesWeights = HugeDoubleArray.newArray(graph.nodeCount());
        encounteredCommunitiesWeights.setAll(c -> -1L);

        return new LocalMovePhase(
            graph,
            communityCount,
            seedCommunities,
            nodeVolumes,
            communityVolumes,
            encounteredCommunities,
            encounteredCommunitiesWeights,
            gamma
        );
    }

    private LocalMovePhase(
        Graph graph,
        long communityCount,
        HugeLongArray seedCommunities,
        HugeDoubleArray nodeVolumes,
        HugeDoubleArray communityVolumes,
        HugeLongArray encounteredCommunities,
        HugeDoubleArray encounteredCommunitiesWeights,
        double gamma
    ) {
        this.graph = graph;
        this.communityCount = communityCount;
        this.currentCommunities = seedCommunities;
        this.gamma = gamma;
        this.nodeVolumes = nodeVolumes;
        this.communityVolumes = communityVolumes;
        this.encounteredCommunities = encounteredCommunities;
        this.encounteredCommunitiesWeights = encounteredCommunitiesWeights;
        this.swaps = 0;

    }

    public Partition run() {
        var queue = createQueue();


        while (!queue.isEmpty()) {
            long nodeId = queue.remove();
            long currentNodeCommunityId = currentCommunities.get(nodeId);
            double currentNodeVolume = nodeVolumes.get(nodeId);


            // Remove the current node volume from its community volume
            communityVolumes.addTo(currentNodeCommunityId, -currentNodeVolume);


            communityRelationshipWeights(nodeId);

            // Compute the "modularity" for the current node and current community
            double currentBestGain = Math.max(
                0,
                encounteredCommunitiesWeights.get(currentNodeCommunityId)
            ) - currentNodeVolume * communityVolumes.get(currentNodeCommunityId) * gamma;


            long bestCommunityId = currentNodeCommunityId;

            bestCommunityId = findBestCommunity(
                currentBestGain,
                currentNodeVolume,
                bestCommunityId
            );

            tryToMoveNode(
                queue,
                nodeId,
                currentNodeCommunityId,
                currentNodeVolume,
                bestCommunityId
            );
        }

        return new Partition(currentCommunities, communityVolumes, communityCount, -1);
    }

    private long findBestCommunity(
        double currentBestGain,
        double currentNodeVolume,
        long bestCommunityId
    ) {

        for (long i = 0; i < encounteredCommunityCounter; ++i) {
            long candidateCommunityId = encounteredCommunities.get(i);
            double candidateCommunityRelationshipsWeight = encounteredCommunitiesWeights.get(candidateCommunityId);
            encounteredCommunitiesWeights.set(candidateCommunityId, -1);
            // Compute the modularity gain for the candidate community
            double modularityGain =
                candidateCommunityRelationshipsWeight - currentNodeVolume * communityVolumes.get(candidateCommunityId) * gamma;

            boolean improves = modularityGain > currentBestGain // gradually better modularity gain
                               // tie-breaking case; consider only positive modularity gains
                               || (modularityGain > 0
                                   // when the current gain is equal to the best gain
                                   && Double.compare(modularityGain, currentBestGain) == 0
                                   // consider it as improvement only if the candidate community ID is lower than the best community ID
                                   // similarly to the Louvain implementation
                                   && candidateCommunityId < bestCommunityId);

            if (improves) {
                bestCommunityId = candidateCommunityId;
                currentBestGain = modularityGain;
            }
        }
        return bestCommunityId;
    }

    private void tryToMoveNode(
        NodesQueue queue,
        long nodeId,
        long currentNodeCommunityId,
        double currentNodeVolume,
        long bestCommunityId
    ) {
        boolean shouldChangeCommunity = bestCommunityId != currentNodeCommunityId;
        if (shouldChangeCommunity) {
            moveNodeToNewCommunity(
                nodeId,
                currentNodeCommunityId,
                bestCommunityId,
                currentNodeVolume
            );

            visitNeighboursAfterMove(nodeId, queue, bestCommunityId);
        } else {
            // We didn't move the node => re-add its degree to its current community sum of degrees
            communityVolumes.addTo(currentNodeCommunityId, currentNodeVolume);
        }
    }

    private void moveNodeToNewCommunity(
        long nodeId,
        long currentNodeCommunityId,
        long newCommunityId,
        double currentNodeVolume
    ) {
        currentCommunities.set(nodeId, newCommunityId);
        communityVolumes.addTo(newCommunityId, currentNodeVolume);
        swaps++;
        if (Double.compare(communityVolumes.get(currentNodeCommunityId), 0.0) == 0) {
            communityCount--;
        }
    }

    private void communityRelationshipWeights(long nodeId) {
        encounteredCommunityCounter = 0;
        graph.forEachRelationship(nodeId, 1.0, (s, t, relationshipWeight) -> {
            long tCommunity = currentCommunities.get(t);
            if (encounteredCommunitiesWeights.get(tCommunity) < 0) {
                encounteredCommunities.set(encounteredCommunityCounter, tCommunity);
                encounteredCommunityCounter++;
                encounteredCommunitiesWeights.set(tCommunity, relationshipWeight);
            } else {
                encounteredCommunitiesWeights.addTo(tCommunity, relationshipWeight);
            }

            return true;
        });
    }

    // all neighbours of the node that do not belong to the node’s new community
    // and that are not yet in the queue are added to the rear of the queue
    private void visitNeighboursAfterMove(long nodeId, NodesQueue queue, long movedToCommunityId) {
        graph.forEachRelationship(nodeId, (s, t) -> {
            long tCommunity = currentCommunities.get(t);
            boolean shouldAddInQueue = !queue.contains(t) && tCommunity != movedToCommunityId;
            if (shouldAddInQueue) {
                queue.add(t);
            }
            return true;
        });
    }

    private NodesQueue createQueue() {
        var queue = new NodesQueue(graph.nodeCount());

        graph.forEachNode(nodeId -> {
            queue.add(nodeId);
            return true;
        });
        return queue;
    }

}
