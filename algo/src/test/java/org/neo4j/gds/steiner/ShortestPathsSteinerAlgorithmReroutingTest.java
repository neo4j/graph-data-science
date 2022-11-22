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
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class ShortestPathsSteinerAlgorithmReroutingTest {
    @GdlGraph(orientation = Orientation.NATURAL)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a4:Node)," +

        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: 4.0}]->(a4)," +

        "  (a1)-[:R {weight: 1.0}]->(a2)," +
        "  (a2)-[:R {weight: 1.0}]->(a3)," +

        "  (a4)-[:R {weight: 0.0}]->(a3)";
    
    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @GdlGraph(graphNamePrefix = "noReroute")
    private static final String nodeCreateQuery =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a1)-[:R {weight: 1.0}]->(a2)," +
        "  (a2)-[:R {weight: 1.0}]->(a3)," +
        "  (a3)-[:R {weight: 0.5}]->(a1),";

    @Inject
    private TestGraph noRerouteGraph;

    @Inject
    private IdFunction noRerouteIdFunction;


    @Test
    void shouldPruneUnusedIfRerouting() {
        var steinerResult = new ShortestPathsSteinerAlgorithm(
            graph,
            idFunction.of("a0"),
            List.of(idFunction.of("a3"), idFunction.of("a4")),
            2.0,
            1,
            false,
            Pools.DEFAULT
        ).compute();
        assertThat(steinerResult.totalCost()).isEqualTo(7.0);
        var steinerResultWithReroute = new ShortestPathsSteinerAlgorithm(
            graph,
            idFunction.of("a0"),
            List.of(idFunction.of("a3"), idFunction.of("a4")),
            2.0,
            1,
            true,
            Pools.DEFAULT
        ).compute();
        assertThat(steinerResultWithReroute.totalCost()).isEqualTo(4.0);

    }



    @Test
    void rerouteShouldNotCreateLoops() {
        var steinerResult = new ShortestPathsSteinerAlgorithm(
            noRerouteGraph,
            noRerouteIdFunction.of("a0"),
            List.of(noRerouteIdFunction.of("a3")),
            2.0,
            1,
            true,
            Pools.DEFAULT
        ).compute();
        var parent = steinerResult.parentArray().toArray();

        assertThat(parent[(int) idFunction.of("a0")]).isEqualTo(ShortestPathsSteinerAlgorithm.ROOTNODE);
        assertThat(parent[(int) idFunction.of("a1")]).isEqualTo(idFunction.of("a0"));
        assertThat(parent[(int) idFunction.of("a2")]).isEqualTo(idFunction.of("a1"));
        assertThat(parent[(int) idFunction.of("a3")]).isEqualTo(idFunction.of("a2"));

        assertThat(steinerResult.totalCost()).isEqualTo(3);

    }

}
