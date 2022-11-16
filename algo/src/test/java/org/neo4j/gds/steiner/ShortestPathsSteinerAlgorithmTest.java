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
package org.neo4j.gds.steiner;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class ShortestPathsSteinerAlgorithmTest {
    @GdlGraph(orientation = Orientation.NATURAL)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a4:Node)," +
        "  (a5:Node)," +
        "  (a6:Node)," +
        "  (a7:Node)," +
        "  (a8:Node)," +
        "  (a9:Node)," +

        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: 1.0}]->(a2)," +
        "  (a0)-[:R {weight: 1.0}]->(a3)," +
        "  (a0)-[:R {weight: 2.0}]->(a9)," +

        "  (a1)-[:R {weight: 1.0}]->(a5)," +


        "  (a2)-[:R {weight: 1.0}]->(a6)," +

        "  (a3)-[:R {weight: 1.0}]->(a4)," +

        "  (a6)-[:R {weight: 1.0}]->(a7)," +
        "  (a6)-[:R {weight: 1.0}]->(a8)," +
        "  (a9)-[:R {weight: 1.0}]->(a6)";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldWorkCorrectly() {
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            graph,
            0L,
            List.of(4L, 7L, 8L),
            1
        ).compute();
        var pruned = ShortestPathsSteinerAlgorithm.PRUNED;
        var rootnode = ShortestPathsSteinerAlgorithm.ROOTNODE;
        long[] parentArray = new long[]{rootnode, pruned, 0, 0, 3, pruned, 2, 6, 6, pruned};
        double[] parentCostArray = new double[]{0, pruned, 1, 1, 1, pruned, 1, 1, 1, pruned};

        assertThat(steinerTreeResult.parentArray().toArray()).isEqualTo(parentArray);
        assertThat(steinerTreeResult.relationshipToParentCost().toArray()).isEqualTo(parentCostArray);
        assertThat(steinerTreeResult.totalCost()).isEqualTo(6.0);


    }

}
