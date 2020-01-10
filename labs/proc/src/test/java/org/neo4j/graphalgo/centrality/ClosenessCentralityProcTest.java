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
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
class ClosenessCentralityProcTest extends BaseProcTest {

    public static final String TYPE = "TYPE";

    private static long centerNodeId;

    interface TestConsumer {
        void accept(long nodeId, double centrality);
    }

    @Mock
    private TestConsumer consumer;

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        DefaultBuilder builder = GraphBuilder.create(db)
                .setLabel("Node")
                .setRelationship(TYPE);

        RelationshipType type = RelationshipType.withName(TYPE);

        /*
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
                .forEachNodeInTx(node -> node.createRelationshipTo(center, type))
                .newRingBuilder()
                .createRing(5)
                .forEachNodeInTx(node -> center.createRelationshipTo(node, type));

        registerProcedures(ClosenessCentralityProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testClosenessStream() {
        String query = gdsCypher()
            .streamMode()
            .yields("nodeId", "centrality");
        runQueryWithRowConsumer(query, row -> {
            consumer.accept(
                row.getNumber("nodeId").longValue(),
                row.getNumber("centrality").doubleValue()
            );
        });

        verifyMock();
    }

    @Test
    void testClosenessWrite() {
        String query = gdsCypher()
            .writeMode()
            .yields("nodes", "loadMillis", "computeMillis", "writeMillis");

        runQueryWithRowConsumer(query, row -> {
            assertNotEquals(-1L, row.getNumber("writeMillis"));
            assertNotEquals(-1L, row.getNumber("loadMillis"));
            assertNotEquals(-1L, row.getNumber("computeMillis"));
            assertNotEquals(-1L, row.getNumber("nodes"));
        });

        runQueryWithRowConsumer("MATCH (n) WHERE exists(n.centrality) RETURN id(n) AS id, n.centrality AS centrality", row -> {
            consumer.accept(
                row.getNumber("id").longValue(),
                row.getNumber("centrality").doubleValue()
            );
        });

        verifyMock();
    }

    private GdsCypher.ModeBuildStage gdsCypher() {
        return GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.closeness");
    }

    private void verifyMock() {
        verify(consumer, times(1)).accept(eq(centerNodeId), AdditionalMatchers.eq(1.0, 0.01));
        verify(consumer, times(10)).accept(anyLong(), AdditionalMatchers.eq(0.588, 0.01));
    }
}
