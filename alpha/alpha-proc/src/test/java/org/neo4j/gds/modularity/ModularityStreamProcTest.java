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
package org.neo4j.gds.modularity;

import org.apache.commons.lang3.mutable.MutableInt;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ModularityStreamProcTest extends BaseProcTest {
    private static final String GRAPH_NAME = "myGraph";

    @Neo4jGraph
    static final String GRAPH =
        "CREATE " +
        " (a1: Node { communityId: 0 })," +
        " (a2: Node { communityId: 0 })," +
        " (a3: Node { communityId: 5 })," +
        " (a4: Node { communityId: 0 })," +
        " (a5: Node { communityId: 5 })," +
        " (a6: Node { communityId: 5 })," +

        " (a1)-[:R]->(a2)," +
        " (a1)-[:R]->(a4)," +
        " (a2)-[:R]->(a3)," +
        " (a2)-[:R]->(a4)," +
        " (a2)-[:R]->(a5)," +
        " (a3)-[:R]->(a6)," +
        " (a4)-[:R]->(a5)," +
        " (a5)-[:R]->(a6)";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            ModularityStreamProc.class,
            GraphProjectProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeProperty("communityId")
            .withAnyLabel()
            .withRelationshipType(
                "R",
                RelationshipProjection.builder().type("R").orientation(Orientation.UNDIRECTED).build()
            )
            .yields();

        runQuery(createQuery);
    }

    @Test
    void stream() {
        String streamQuery = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.modularity")
            .streamMode()
            .addParameter("communityProperty", "communityId")
            .yields();

        var expected = Map.of(
            0L, (6 - 9 * 9 * (1.0 / 16)) / 16,
            5L, (4 - 7 * 7 * (1.0 / 16)) / 16
        );

        var rowCount = new MutableInt();
        runQueryWithRowConsumer(streamQuery, row -> {
            long community = row.getNumber("communityId").longValue();
            double conductance = row.getNumber("modularity").doubleValue();
            assertThat(conductance).isCloseTo(expected.get(community), Offset.offset(0.0001));
            rowCount.increment();
        });

        assertThat(rowCount.intValue())
            .as("Result count doesn't match")
            .isEqualTo(2);
    }
}
