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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.betweenness.BetweennessCentrality;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ParallelBetweennessCentralityProcTest extends BaseProcTest {

    public static final String TYPE = "TYPE";

    private long centerNodeId;

    @Mock
    private BetweennessCentrality.ResultConsumer consumer;

    @BeforeEach
    void setupGraph() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();;

        DefaultBuilder builder = GraphBuilder.create(db)
            .setLabel("Node")
            .setRelationship(TYPE);

        final RelationshipType type = RelationshipType.withName(TYPE);

        /**
         * create two rings of nodes where each node of ring A
         * is connected to center while center is connected to
         * each node of ring B.
         */
        final Node center = builder.newDefaultBuilder()
                .setLabel("Node")
                .createNode();

        centerNodeId = center.getId();

        builder.newRingBuilder()
                .createRing(5)
                .forEachNodeInTx(node -> {
                    node.createRelationshipTo(center, type);
                })
                .newRingBuilder()
                .createRing(5)
                .forEachNodeInTx(node -> {
                    center.createRelationshipTo(node, type);
                });

        registerProcedures(BetweennessCentralityProc.class);
        when(consumer.consume(anyLong(), anyDouble()))
            .thenReturn(true);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testParallelBC() {

        String cypher = "CALL algo.betweenness('', '', {concurrency:4, write:true, writeProperty:'bc', stats:true}) YIELD " +
                "loadMillis, computeMillis, writeMillis, nodes, minCentrality, maxCentrality, sumCentrality";

        testBetweennessWrite(cypher);
    }

    @Test
    void testBC() {

        String cypher = "CALL algo.betweenness('', '', {write:true, writeProperty:'bc', stats:true}) YIELD " +
                "loadMillis, computeMillis, writeMillis, nodes, minCentrality, maxCentrality, sumCentrality";

        testBetweennessWrite(cypher);
    }

    void testBetweennessWrite(String cypher) {
        runQuery(cypher, row -> {
            assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
            assertEquals(6.0, row.getNumber("minCentrality"));
            assertEquals(25.0, row.getNumber("maxCentrality"));
            assertEquals(85.0, row.getNumber("sumCentrality"));
        });

        runQuery("MATCH (n:Node) WHERE exists(n.bc) RETURN id(n) as id, n.bc as bc", row -> consumer.consume(
            row.getNumber("id").longValue(),
            row.getNumber("bc").doubleValue()
        ));

        verify(consumer, times(10)).consume(anyLong(), eq(6.0));
        verify(consumer, times(1)).consume(eq(centerNodeId), eq(25.0));
    }

}
