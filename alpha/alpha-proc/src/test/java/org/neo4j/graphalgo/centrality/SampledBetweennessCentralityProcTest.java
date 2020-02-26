/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

package org.neo4j.graphalgo.centrality;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.betweenness.BetweennessCentrality;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SampledBetweennessCentralityProcTest extends BaseProcTest {

    private static final RelationshipType TYPE = RelationshipType.withName("TYPE");

    private static long centerNodeId;

    @Mock
    private BetweennessCentrality.ResultConsumer consumer;

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(SampledBetweennessCentralityProc.class);

        DefaultBuilder builder = GraphBuilder.create(db)
            .setLabel("Node")
            .setRelationship(TYPE.name());

        /**
         * create two rings of nodes where each node of ring A
         * is connected to center while center is connected to
         * each node of ring B.
         */
        Node center = builder.newDefaultBuilder()
            .setLabel("Node")
            .createNode();

        centerNodeId = center.getId();

        builder.newRingBuilder()
            .createRing(5)
            .forEachNodeInTx(node -> node.createRelationshipTo(center, TYPE))
            .newRingBuilder()
            .createRing(5)
            .forEachNodeInTx(node -> center.createRelationshipTo(node, TYPE))
            .close();
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testRABrandesHighProbability() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.betweenness.sampled")
            .writeMode()
            .addParameter("strategy", "random")
            .addParameter("probability", 1.0)
            .yields(
                "nodes",
                "minCentrality",
                "maxCentrality",
                "sumCentrality",
                "createMillis",
                "computeMillis",
                "writeMillis"
            );
        runQueryWithRowConsumer(query, row -> {
                assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.1);
                assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.1);
                assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.1);
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    @Test
    void testRABrandesNoProbability() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.betweenness.sampled")
            .writeMode()
            .addParameter("strategy", "random")
            .yields(
                "nodes",
                "minCentrality",
                "maxCentrality",
                "sumCentrality",
                "createMillis",
                "computeMillis",
                "writeMillis"
            );
        runQueryWithRowConsumer(query, row -> {
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    @Test
    void testRABrandeseWrite() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.betweenness.sampled")
            .writeMode()
            .addParameter("strategy", "random")
            .addParameter("probability", 1.0)
            .yields(
                "nodes",
                "minCentrality",
                "maxCentrality",
                "sumCentrality",
                "createMillis",
                "computeMillis",
                "writeMillis"
            );
        runQueryWithRowConsumer(query, row -> {
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    @Test
    void testRABrandesStream() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.betweenness.sampled")
            .streamMode()
            .addParameter("strategy", "random")
            .addParameter("probability", 1.0)
            .yields("nodeId", "centrality");
        runQueryWithRowConsumer(query, row -> consumer.consume(
                row.getNumber("nodeId").intValue(),
                row.getNumber("centrality").doubleValue()
            )
        );

        verify(consumer, times(10)).consume(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(6.0));
        verify(consumer, times(1)).consume(ArgumentMatchers.eq(centerNodeId), ArgumentMatchers.eq(25.0));
    }

}
