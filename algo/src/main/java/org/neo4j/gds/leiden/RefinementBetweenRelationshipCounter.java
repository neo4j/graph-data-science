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
import org.neo4j.gds.core.utils.partition.Partition;

public class RefinementBetweenRelationshipCounter implements Runnable {
    private final Graph graph;
    private final Partition partition;
    private final HugeDoubleArray relationshipsBetweenCommunities;

    private final HugeLongArray originalCommunities;


    public RefinementBetweenRelationshipCounter(
        Graph graph,
        HugeDoubleArray relationshipsBetweenCommunities,
        HugeLongArray originalCommunities,
        Partition partition
    ) {
        this.graph = graph;
        this.relationshipsBetweenCommunities = relationshipsBetweenCommunities;
        this.partition = partition;
        this.originalCommunities = originalCommunities;

    }

    @Override
    public void run() {
        long startNode = partition.startNode();
        long endNode = startNode + partition.nodeCount();
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {
            long originalCommunityId = originalCommunities.get(nodeId);
            final long finalNodeId = nodeId;
            graph.forEachRelationship(nodeId, 1.0, (s, t, relationshipWeight) -> {
                var tOriginalCommunityId = originalCommunities.get(t);
                if (originalCommunityId == tOriginalCommunityId) {
                    relationshipsBetweenCommunities.addTo(finalNodeId, relationshipWeight);
                }
                return true;
            });
        }
    }
}
