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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.betweenness.BetweennessCentrality;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BetweennessCentralityProcTest extends BaseProcTest {

    private static final RelationshipType TYPE = RelationshipType.withName("TYPE");

    private static long centerNodeId;

    @Mock
    private BetweennessCentrality.ResultConsumer consumer;

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(BetweennessCentralityProc.class);

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
            .forEachNodeInTx(node -> center.createRelationshipTo(node, TYPE));
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testBetweennessStream() {
        String query = gdsCypher()
            .streamMode()
            .addParameter("concurrency", 1)
            .yields("nodeId", "centrality");

        runQueryWithRowConsumer(query, row -> consumer.consume(
                row.getNumber("nodeId").longValue(),
                row.getNumber("centrality").doubleValue()
            )
        );
        verify(consumer, times(10)).consume(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(6.0));
        verify(consumer, times(1)).consume(ArgumentMatchers.eq(centerNodeId), ArgumentMatchers.eq(25.0));
    }

    @Test
    void testParallelBetweennessStream() {
        String query = gdsCypher()
            .streamMode()
            .addParameter("concurrency", 4)
            .yields("nodeId", "centrality");
        runQueryWithRowConsumer(query, row -> consumer.consume(
                row.getNumber("nodeId").intValue(),
                row.getNumber("centrality").doubleValue()
            )
        );
        verify(consumer, times(10)).consume(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(6.0));
        verify(consumer, times(1)).consume(ArgumentMatchers.eq(centerNodeId), ArgumentMatchers.eq(25.0));
    }

    @Test
    void testParallelBetweennessWrite() {
        String query = gdsCypher()
            .writeMode()
            .addParameter("concurrency", 4)
            .yields("nodes", "minCentrality", "maxCentrality", "sumCentrality", "loadMillis", "computeMillis", "writeMillis");
        runQueryWithRowConsumer(query, row -> {
                assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.01);
                assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.01);
                assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.01);
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    @Test
    void testParallelBetweennessWriteWithDirection() {
        String query = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", RelationshipProjection.builder().projection(Projection.UNDIRECTED).build())
            .algo("gds.alpha.betweenness")
            .writeMode()
            .addParameter("concurrency", 4)
            .addParameter("direction", "BOTH")
            .yields("nodes", "minCentrality", "maxCentrality", "sumCentrality", "loadMillis", "computeMillis", "writeMillis");
        runQueryWithRowConsumer(query, row -> {
                assertEquals(35.0, (double) row.getNumber("sumCentrality"), 0.01);
                assertEquals(30.0, (double) row.getNumber("maxCentrality"), 0.01);
                assertEquals(0.5, (double) row.getNumber("minCentrality"), 0.01);
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    @Test
    void testBetweennessWrite() {
        String query = gdsCypher()
            .writeMode()
            .addParameter("concurrency", 1)
            .yields("nodes",
                "minCentrality",
                "maxCentrality",
                "sumCentrality",
                "loadMillis",
                "computeMillis",
                "writeMillis"
            );
        runQueryWithRowConsumer(query, row -> {
                assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.01);
                assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.01);
                assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.01);
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    @Test
    void testBetweennessWriteWithDirection() {
        String query = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", RelationshipProjection.builder().projection(Projection.UNDIRECTED).build())
            .algo("gds.alpha.betweenness")
            .writeMode()
            .addParameter("direction", "BOTH")
            .addParameter("concurrency", 4)
            .yields("nodes", "minCentrality", "maxCentrality", "sumCentrality", "loadMillis", "computeMillis", "writeMillis");
        runQueryWithRowConsumer(query, row -> {
                assertEquals(35.0, (double) row.getNumber("sumCentrality"), 0.01);
                assertEquals(30.0, (double) row.getNumber("maxCentrality"), 0.01);
                assertEquals(0.5, (double) row.getNumber("minCentrality"), 0.01);
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    // TODO: Disable until there is a proc for sampled betweenness centrality
    @Disabled
    @Test
    void testRABrandesHighProbability(String graphName) {
        String query = "CALL algo.betweenness.sampled('','', {graph: $graph,strategy:'random', probability:1.0, write:true, " +
                       "stats:true, writeProperty:'centrality'}) YIELD " +
                       "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        runQueryWithRowConsumer(query, Collections.singletonMap("graph", graphName),
            row -> {
                assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.1);
                assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.1);
                assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.1);
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    // TODO: Disable until there is a proc for sampled betweenness centrality
    @Disabled
    @Test
    void testRABrandesNoProbability(String graphName) {
        String query = "CALL algo.betweenness.sampled('','', {graph: $graph,strategy:'random', write:true, stats:true, " +
                       "writeProperty:'centrality'}) YIELD " +
                       "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        runQueryWithRowConsumer(query, Collections.singletonMap("graph", graphName),
            row -> {
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    // TODO: Disable until there is a proc for sampled betweenness centrality
    @Disabled
    @Test
    void testRABrandeseWrite(String graphName) {
        String query = "CALL algo.betweenness.sampled('','', {graph: $graph,strategy:'random', probability:1.0, " +
                       "write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                       "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        runQueryWithRowConsumer(query, Collections.singletonMap("graph", graphName),
            row -> {
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("nodes"));
            }
        );
    }

    // TODO: Disable until there is a proc for sampled betweenness centrality
    @Disabled
    @Test
    void testRABrandesStream(String graphName) {
        String query = "CALL algo.betweenness.sampled.stream('','', {graph: $graph, strategy:'random', probability:1.0, " +
                       "write:true, stats:true, writeProperty:'centrality'}) YIELD nodeId, centrality";
        runQueryWithRowConsumer(query, Collections.singletonMap("graph", graphName),
            row -> {
                consumer.consume(
                    row.getNumber("nodeId").intValue(),
                    row.getNumber("centrality").doubleValue()
                );
            }
        );

        verify(consumer, times(10)).consume(ArgumentMatchers.anyLong(), ArgumentMatchers.eq(6.0));
        verify(consumer, times(1)).consume(ArgumentMatchers.eq(centerNodeId), ArgumentMatchers.eq(25.0));
    }

    private GdsCypher.ModeBuildStage gdsCypher() {
        return GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE")
            .algo("gds.alpha.betweenness");
    }

}
