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

final class ModularityComputer {

    private ModularityComputer() {}

    static double modularity(Graph graph, HugeLongArray finalCommunities, double gamma) {
        double modularity = 0d;
        HugeDoubleArray sumOfEdges = HugeDoubleArray.newArray(graph.nodeCount());
        HugeDoubleArray insideEdges = HugeDoubleArray.newArray(graph.nodeCount());
        double coefficient = 1.0 / graph.relationshipCount();
        graph.forEachNode(
            nodeId -> {
                long communityId = finalCommunities.get(nodeId);
                graph.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                    long tCommunityId = finalCommunities.get(t);
                    if (communityId == tCommunityId)
                        insideEdges.addTo(communityId, w);
                    sumOfEdges.addTo(communityId, w);
                    return true;
                });
                return true;
            }
        );
        for (long community = 0; community < graph.nodeCount(); ++community) {
            double ec = insideEdges.get(community);
            double Kc = sumOfEdges.get(community);
            modularity += (ec - Kc * Kc * gamma);
        }
        return modularity * coefficient;
    }
}
