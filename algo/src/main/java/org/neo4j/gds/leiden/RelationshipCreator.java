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
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

final class RelationshipCreator implements Runnable {

    private final Orientation orientation;
    private final RelationshipsBuilder relationshipsBuilder;
    private final HugeLongArray communities;
    private final RelationshipIterator relationshipIterator;
    private final Partition partition;
    private final ProgressTracker progressTracker;
    private final HugeDoubleArray encounteredCommunityWeights;
    private final HugeLongArray encounteredCommunities;

    private long encountereCommunitiesCounter;

    RelationshipCreator(
        HugeLongArray communities,
        Partition partition,
        RelationshipsBuilder relationshipsBuilder,
        RelationshipIterator relationshipIterator,
        Orientation orientation,
        ProgressTracker progressTracker
    ) {
        this.orientation = orientation;
        this.relationshipsBuilder = relationshipsBuilder;
        this.communities = communities;
        this.relationshipIterator = relationshipIterator;
        this.partition = partition;
        this.progressTracker = progressTracker;
        this.encounteredCommunityWeights = HugeDoubleArray.newArray(communities.size());
        this.encounteredCommunities = HugeLongArray.newArray(communities.size());
        encounteredCommunityWeights.setAll(v -> -1);
    }


    private void updateRelationships(long sourceCommunityId, long numEncounteredCommunities) {
        for (long communityIndexId = 0; communityIndexId < numEncounteredCommunities; ++communityIndexId) {
            long communityId = encounteredCommunities.get(communityIndexId);
            double weight = encounteredCommunityWeights.get(communityId);
            relationshipsBuilder.add(sourceCommunityId, communityId, weight);
            encounteredCommunityWeights.set(communityId, -1);
        }
    }

    @Override
    public void run() {
        HugeLongArray sortedByCommunity = getNodesSortedByCommunity(partition, communities);
        long partitionCount = partition.nodeCount();
        long previousCommunity = -1;
        for (long nodeIndexId = 0; nodeIndexId < partitionCount; ++nodeIndexId) {
            long nodeId = sortedByCommunity.get(nodeIndexId);
            long currentCommunity = communities.get(nodeId);

            boolean shouldUpdate = (nodeIndexId > 0 && previousCommunity != currentCommunity);

            if (shouldUpdate) {
                updateRelationships(previousCommunity, encountereCommunitiesCounter);
                encountereCommunitiesCounter = 0;
            }

            relationshipIterator.forEachRelationship(nodeId, 1.0, (source, target, property) -> {
                // do not allow self-loops
                long targetCommunityId = communities.get(target);
                if (currentCommunity != targetCommunityId) { //we do not include  self-edges in next graph
                    //if orientation is natural it means a-[weight]->b will be examined only from a
                    // hence we should add it as we encounter it from a
                    //otherwise  a<-[weight]->b will be visited from both a and b.
                    // To not include it twice we break a tie based on id.
                    if (orientation == Orientation.NATURAL || currentCommunity > targetCommunityId) {
                        double valueToAdd = property;
                        if (encounteredCommunityWeights.get(targetCommunityId) == -1) {
                            encounteredCommunities.set(encountereCommunitiesCounter++, targetCommunityId);
                            valueToAdd++;
                        }
                        encounteredCommunityWeights.addTo(targetCommunityId, valueToAdd);
                    }
                }
                return true;
            });

            shouldUpdate = (nodeIndexId == (partitionCount - 1));
            if (shouldUpdate) {
                updateRelationships(currentCommunity, encountereCommunitiesCounter);
            }
            previousCommunity = currentCommunity;
            progressTracker.logProgress();
        }

    }

    static HugeLongArray getNodesSortedByCommunity(Partition partition, HugeLongArray communities) {
        long nodeCount = communities.size();
        long partitionCount = partition.nodeCount();
        var sortedNodesByCommunity = HugeLongArray.newArray(partitionCount);
        HugeLongArray communityCoordinateArray = HugeLongArray.newArray(nodeCount);

        HugeLongArray indexMap = HugeLongArray.newArray(nodeCount);
        
        BitSet setCommunityCoordinates = new BitSet(nodeCount + 1);

        long endNode = partition.startNode() + partition.nodeCount();
        for (long nodeId = partition.startNode(); nodeId < endNode; ++nodeId) {
            long communityId = communities.get(nodeId);
            setCommunityCoordinates.getAndSet(communityId);
            communityCoordinateArray.addTo(communityId, 1);
        }


        long nodeSum = 0;
        long bitIndex = setCommunityCoordinates.nextSetBit(0);
        long currentId = 0;
        while (bitIndex != -1) {
            indexMap.set(bitIndex, currentId);
            nodeSum += communityCoordinateArray.get(bitIndex);
            communityCoordinateArray.set(currentId++, nodeSum);
            bitIndex = setCommunityCoordinates.nextSetBit(bitIndex + 1);
        }

        for (long nodeId = endNode - 1; nodeId >= partition.startNode(); --nodeId) {
            long community = communities.get(nodeId);
            long communityId = indexMap.get(community);
            long coordinate = communityCoordinateArray.get(communityId) - 1;
            sortedNodesByCommunity.set(coordinate, nodeId);
            communityCoordinateArray.set(communityId, coordinate);
        }
        return sortedNodesByCommunity;

    }
}

