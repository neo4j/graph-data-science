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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.betweenness.BetweennessCentrality;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * @author mknblch
 */
public class BetweennessCentralityIntegrationTest extends GraphTester {

    public static final String TYPE = "TYPE";
    private final String graphName;

    private static DefaultBuilder builder;
    private static long centerNodeId;

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private BetweennessCentrality.ResultConsumer consumer;

    @Before
    public void setupMocks() {
        when(consumer.consume(Matchers.anyLong(), Matchers.anyDouble()))
                .thenReturn(true);
    }

    @BeforeClass
    public static void setupGraph() throws KernelException {

        builder = GraphBuilder.create(db)
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

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(BetweennessCentralityProc.class);
    }

    public BetweennessCentralityIntegrationTest(final Class<? extends GraphFactory> graphImpl, String name) {
        super(graphImpl);
        this.graphName = name;
    }

    @Test
    public void testBetweennessStream() throws Exception {
        String query = "CALL algo.betweenness.stream('Node', 'TYPE', {graph: $graph}) YIELD nodeId, centrality";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.consume(
                            (long) row.getNumber("nodeId"),
                            (double) row.getNumber("centrality"));
                    return true;
                });
        verify(consumer, times(10)).consume(Matchers.anyLong(), Matchers.eq(6.0));
        verify(consumer, times(1)).consume(Matchers.eq(centerNodeId), Matchers.eq(25.0));
    }

    @Test
    public void testParallelBetweennessStream() throws Exception {
        String query = "CALL algo.betweenness.stream('Node', 'TYPE', {graph: $graph, concurrency: 4}) YIELD nodeId, centrality";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.consume(
                            row.getNumber("nodeId").intValue(),
                            row.getNumber("centrality").doubleValue());
                    return true;
                });

        verify(consumer, times(10)).consume(Matchers.anyLong(), Matchers.eq(6.0));
        verify(consumer, times(1)).consume(Matchers.eq(centerNodeId), Matchers.eq(25.0));
    }

    @Test
    public void testParallelBetweennessWrite() throws Exception {
        String query = "CALL algo.betweenness('','', {graph: $graph, concurrency:4, write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.01);
                    assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.01);
                    assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.01);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testParallelBetweennessWriteWithDirection() throws Exception {
        String query = "CALL algo.betweenness('','', {graph: $graph, direction:'<>', concurrency:4, write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(35.0, (double) row.getNumber("sumCentrality"), 0.01);
                    assertEquals(30.0, (double) row.getNumber("maxCentrality"), 0.01);
                    assertEquals(0.5, (double) row.getNumber("minCentrality"), 0.01);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testBetweennessWrite() throws Exception {
        String query = "CALL algo.betweenness('','', {graph: $graph, write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.01);
                    assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.01);
                    assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.01);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testBetweennessWriteWithDirection() throws Exception {
        String query = "CALL algo.betweenness('','', {graph: $graph, direction:'both', write:true, stats:true, writeProperty:'centrality'}) " +
                "YIELD nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(35.0, (double) row.getNumber("sumCentrality"), 0.01);
                    assertEquals(30.0, (double) row.getNumber("maxCentrality"), 0.01);
                    assertEquals(0.5, (double) row.getNumber("minCentrality"), 0.01);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testRABrandesHighProbability() throws Exception {
        String query = "CALL algo.betweenness.sampled('','', {graph: $graph,strategy:'random', probability:1.0, write:true, " +
                "stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(85.0, (double) row.getNumber("sumCentrality"), 0.1);
                    assertEquals(25.0, (double) row.getNumber("maxCentrality"), 0.1);
                    assertEquals(6.0, (double) row.getNumber("minCentrality"), 0.1);
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testRABrandesNoProbability() throws Exception {
        String query = "CALL algo.betweenness.sampled('','', {graph: $graph,strategy:'random', write:true, stats:true, " +
                "writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testRABrandeseWrite() throws Exception {
        String query = "CALL algo.betweenness.sampled('','', {graph: $graph,strategy:'random', probability:1.0, " +
                "write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, minCentrality, maxCentrality, sumCentrality, loadMillis, computeMillis, writeMillis";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testRABrandesStream() throws Exception {
        String query = "CALL algo.betweenness.sampled.stream('','', {graph: $graph, strategy:'random', probability:1.0, " +
                "write:true, stats:true, writeProperty:'centrality'}) YIELD nodeId, centrality";
        db.execute(query, Collections.singletonMap("graph", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.consume(
                            row.getNumber("nodeId").intValue(),
                            row.getNumber("centrality").doubleValue());
                    return true;
                });

        verify(consumer, times(10)).consume(Matchers.anyLong(), Matchers.eq(6.0));
        verify(consumer, times(1)).consume(Matchers.eq(centerNodeId), Matchers.eq(25.0));
    }


}
