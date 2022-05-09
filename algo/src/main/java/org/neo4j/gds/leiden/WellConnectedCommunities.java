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

import java.util.concurrent.atomic.DoubleAdder;

public class WellConnectedCommunities {
    boolean test(
        Graph graph,
        long originalCommunityId,
        long updatedCommunityId,
        HugeLongArray originalCommunities,
        HugeLongArray updatedCommunities,
        HugeDoubleArray originalCommunityVolumes,
        HugeDoubleArray updatedCommunityVolumes,
        double gamma
    ) {
        double relationshipsOutsideTheCommunity = relationshipsBetweenCommunities(
            graph.concurrentCopy(),
            updatedCommunityId,
            originalCommunities,
            updatedCommunities
        );
        double originalCommunityVolume = originalCommunityVolumes.get(originalCommunityId);
        double updatedCommunityVolume = updatedCommunityVolumes.get(updatedCommunityId);
        double rightSide = gamma * updatedCommunityVolume * (originalCommunityVolume - updatedCommunityVolume);

        return relationshipsOutsideTheCommunity >= rightSide;
    }

    double relationshipsBetweenCommunities(Graph graph, long communityId, HugeLongArray originalCommunities, HugeLongArray updatedCommunities) {
        DoubleAdder relationshipsCountBetweenCommunities = new DoubleAdder();
        graph.forEachNode(nodeId -> {
            long originalCommunityId = originalCommunities.get(nodeId);
            if (updatedCommunities.get(nodeId) == communityId) {
                graph.forEachRelationship(nodeId, 1.0, (s, t, relationshipWeight) -> {
                    var tOriginalCommunityId = originalCommunities.get(t);
                    var tUpdatedCommunity = updatedCommunities.get(t);
                    if(originalCommunityId == tOriginalCommunityId && communityId != tUpdatedCommunity) {
                        relationshipsCountBetweenCommunities.add(relationshipWeight);
                    }

                    return true;
                });
            }
            return true;
        });

        return relationshipsCountBetweenCommunities.doubleValue();
    }
}
