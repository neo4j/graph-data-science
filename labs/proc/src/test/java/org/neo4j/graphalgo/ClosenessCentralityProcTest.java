/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
class ClosenessCentralityProcTest extends ProcTestBase {

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
    void testClosenessStream() throws Exception {
        runQuery("CALL algo.closeness.stream('Node', 'TYPE') YIELD nodeId, centrality", row -> {
            consumer.accept(
                row.getNumber("nodeId").longValue(),
                row.getNumber("centrality").doubleValue()
            );
        });

        verifyMock();
    }

    @Test
    void testClosenessWrite() throws Exception {
        runQuery("CALL algo.closeness('','', {write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                 "nodes, loadMillis, computeMillis, writeMillis", row -> {
            assertNotEquals(-1L, row.getNumber("writeMillis"));
            assertNotEquals(-1L, row.getNumber("computeMillis"));
            assertNotEquals(-1L, row.getNumber("nodes"));
        });

        runQuery("MATCH (n) WHERE exists(n.centrality) RETURN id(n) as id, n.centrality as centrality", row -> {
            System.out.println(
                row.getNumber("id").longValue() + " " +
                row.getNumber("centrality").doubleValue());
            consumer.accept(
                row.getNumber("id").longValue(),
                row.getNumber("centrality").doubleValue()
            );
        });

        verifyMock();
    }

    private void verifyMock() {
        verify(consumer, times(1)).accept(eq(centerNodeId), AdditionalMatchers.eq(1.0, 0.01));
        verify(consumer, times(10)).accept(anyLong(), AdditionalMatchers.eq(0.588, 0.01));
    }
}
