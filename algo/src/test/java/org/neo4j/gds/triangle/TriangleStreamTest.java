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
package org.neo4j.gds.triangle;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class TriangleStreamTest {
    /*
     * graph is a  ring  with a center node
     *          center
     *        | | | | | |
     *       a-b-c-d-e-f
     *       |---------|
     *
     */
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
            "  (a0:node)," +
            "  (a1:node)," +
            "  (a2:node)," +
            "  (a3:node)," +
            "  (a4:node)," +
            "  (a5:node)," +
            "  (a6:node)," +
            "  (a7:node)," +
            "  (aCenter:node)," + //put in middle so that it doesnt have to do all the work for al triangles
            "  (a8:node)," +
            "  (a9:node)," +
            "  (a10:node)," +
            "  (a11:node)," +
            "  (a12:node)," +
            "  (a0)-[:R ]->(a1)," +
            "  (a1)-[:R ]->(a2)," +
            "  (a2)-[:R ]->(a3)," +
            "  (a3)-[:R ]->(a4)," +
            "  (a4)-[:R ]->(a5)," +
            "  (a5)-[:R ]->(a6)," +
            "  (a6)-[:R ]->(a7)," +
            "  (a7)-[:R ]->(a8)," +
            "  (a8)-[:R ]->(a9)," +
            "  (a9)-[:R ]->(a10)," +
            "  (a10)-[:R ]->(a11)," +
            "  (a11)-[:R ]->(a12)," +
            "  (a12)-[:R ]->(a0)," +
            "  (aCenter)-[:R ]->(a1)," +
            "  (aCenter)-[:R ]->(a2)," +
            "  (aCenter)-[:R ]->(a3)," +
            "  (aCenter)-[:R ]->(a4)," +
            "  (aCenter)-[:R ]->(a5)," +
            "  (aCenter)-[:R ]->(a6)," +
            "  (aCenter)-[:R ]->(a7)," +
            "  (aCenter)-[:R ]->(a8)," +
            "  (aCenter)-[:R ]->(a9)," +
            "  (aCenter)-[:R ]->(a10)," +
            "  (aCenter)-[:R ]->(a11)," +
            "  (aCenter)-[:R ]->(a12)," +
            "  (aCenter)-[:R ]->(a0)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldListTrianglesCorrectly(int concurrency) {
        var centerId = idFunction.of("aCenter");
        AtomicInteger centerAppearances = new AtomicInteger();
        AtomicInteger allAppearances = new AtomicInteger();

        TriangleStream.create(graph, DefaultPool.INSTANCE, concurrency)
            .compute()
            .forEach(r -> {
                if (r.nodeA == centerId || r.nodeB == centerId || r.nodeC == centerId) {
                    centerAppearances.getAndIncrement();
                }
                allAppearances.getAndIncrement();
            });

        assertThat(centerAppearances.intValue()).isEqualTo(allAppearances.intValue()).isEqualTo(13);
    }
}
