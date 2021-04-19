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
package org.neo4j.graphalgo.influenceΜaximization;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *     (c)-----|
 *    /(d)\----|-|
 *   //(e)\\---|-|-|
 *  ///(f)\\\--|-|-|-|
 * ////   \\\\ | | | |
 * (a)     (b) | | | |
 * \\\\   //// | | | |
 *  \\\(g)///--| | | |
 *   \\(h)//-----| | |
 *    \(i)/--------| |
 *     (j)-----------|
 */
class GreedyProcTest extends BaseProcTest {
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        ", (j:Node)" +

        ", (a)-[:RELATIONSHIP]->(c)" +
        ", (a)-[:RELATIONSHIP]->(d)" +
        ", (a)-[:RELATIONSHIP]->(e)" +
        ", (a)-[:RELATIONSHIP]->(f)" +
        ", (a)-[:RELATIONSHIP]->(g)" +
        ", (a)-[:RELATIONSHIP]->(h)" +
        ", (a)-[:RELATIONSHIP]->(i)" +
        ", (a)-[:RELATIONSHIP]->(j)" +

        ", (b)-[:RELATIONSHIP]->(c)" +
        ", (b)-[:RELATIONSHIP]->(d)" +
        ", (b)-[:RELATIONSHIP]->(e)" +
        ", (b)-[:RELATIONSHIP]->(f)" +
        ", (b)-[:RELATIONSHIP]->(g)" +
        ", (b)-[:RELATIONSHIP]->(h)" +
        ", (b)-[:RELATIONSHIP]->(i)" +
        ", (b)-[:RELATIONSHIP]->(j)" +

        ", (c)-[:RELATIONSHIP]->(g)" +
        ", (d)-[:RELATIONSHIP]->(h)" +
        ", (e)-[:RELATIONSHIP]->(i)" +
        ", (f)-[:RELATIONSHIP]->(j)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GreedyProc.class, GraphCreateProc.class);
        runQuery(DB_CYPHER);

        String graphCreateQuery = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType(
                "RELATIONSHIP",
                RelationshipProjection.of(
                    "RELATIONSHIP",
                    Orientation.NATURAL,
                    Aggregation.DEFAULT
                )
            ).graphCreate("greedyGraph")
            .yields();

        runQuery(graphCreateQuery);
    }

    @AfterEach
    void shutdownGraph() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testResultStream() {
        final Consumer consumer = mock(Consumer.class);

        final String cypher = "CALL gds.alpha.influenceMaximization.greedy.stream" +
                              "('greedyGraph'," +
                              "{" +
                              "   k:  2," +
                              "   p:  0.2," +
                              "   mc: 10," +
                              "   concurrency: 2" +
                              "})" +
                              "YIELD nodeId, spread RETURN nodeId, spread";

        runQueryWithRowConsumer(cypher, (tx, row) -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double spread = row.getNumber("spread").doubleValue();
            consumer.accept(nodeId, spread);
        });

        verify(consumer, times(1)).accept(0L, 2.2d);
        verify(consumer, times(1)).accept(1L, 4.4d);
    }

    interface Consumer {
        void accept(long nodeId, double spread);
    }
}
