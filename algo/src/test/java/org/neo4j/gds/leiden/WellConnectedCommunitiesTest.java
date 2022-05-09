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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class WellConnectedCommunitiesTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a4:Node)," +
        "  (a0)-[:R]->(a1)," +
        "  (a0)-[:R]->(a2)," +
        "  (a0)-[:R]->(a3)," +
        "  (a0)-[:R]->(a4)," +
        "  (a2)-[:R]->(a3)," +
        "  (a2)-[:R]->(a4)," +
        "  (a2)-[:R]->(a1)," +
        "  (a3)-[:R]->(a4)";

    @Inject
    private TestGraph graph;

    @Test
    void testRelationshipsBetweenCommunities() {
        var predicate = new WellConnectedCommunities();

        // We start with two communities - (a0, a1) and (a2, a3, a4)
        var originalCommunities = HugeLongArray.of(1, 1, 0, 0, 0);
        // Node `a3` changes community
        var updatedCommunities = HugeLongArray.of(1, 1, 0, 2, 0);

        // compute the relationships between community `0` and newly created communities, note that community `1` doesn't change
        var relationshipsBetweenCommunities =
            predicate.relationshipsBetweenCommunities(graph, 2, originalCommunities, updatedCommunities);

        // we get two relationships between community `0` and community `2` --> (a2)-->(a3) and (a4)-->(a3)
        assertThat(relationshipsBetweenCommunities).isEqualTo(2L);

    }

    @Test
    void testWellConnectedness() {
        var predicate = new WellConnectedCommunities();

        // We start with two communities - (a0, a1) and (a2, a3, a4)
        var originalCommunities = HugeLongArray.of(1, 1, 0, 0, 0);
        var originalCommunityVolumes = HugeDoubleArray.of(3, 2, 0, 0, 0);
        // Node `a3` changes community
        var updatedCommunities = HugeLongArray.of(1, 1, 0, 2, 0);
        var updatedCommunityVolumes = HugeDoubleArray.of(2, 2, 1, 0, 0);

        assertThat(predicate.test(
            graph,
            0,
            2,
            originalCommunities,
            updatedCommunities,
            originalCommunityVolumes,
            updatedCommunityVolumes,
            1.0
        )).isTrue();
    }
}
