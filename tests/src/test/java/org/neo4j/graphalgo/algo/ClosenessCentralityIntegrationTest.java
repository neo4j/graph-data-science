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
package org.neo4j.graphalgo.algo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.ClosenessCentralityProc;
import org.neo4j.graphalgo.helper.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class ClosenessCentralityIntegrationTest {

    public static final String TYPE = "TYPE";

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private static long centerNodeId;

    interface TestConsumer {
        void accept(long nodeId, double centrality);
    }

    @Mock
    private TestConsumer consumer;

    @BeforeClass
    public static void setupGraph() throws KernelException {
        final DefaultBuilder builder = GraphBuilder.create(DB)
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

        DB.resolveDependency(Procedures.class).registerProcedure(ClosenessCentralityProc.class);
    }

    @Test
    public void testClosenessStream() throws Exception {
        DB.execute("CALL algo.closeness.stream('Node', 'TYPE') YIELD nodeId, centrality")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.accept(
                            row.getNumber("nodeId").longValue(),
                            row.getNumber("centrality").doubleValue());
                    return true;
                });

        verifyMock();
    }

    @Test
    public void testClosenessWrite() throws Exception {
        DB.execute("CALL algo.closeness('','', {write:true, stats:true, writeProperty:'centrality'}) YIELD " +
                "nodes, loadMillis, computeMillis, writeMillis")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("computeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    return true;
                });

        DB.execute("MATCH (n) WHERE exists(n.centrality) RETURN id(n) as id, n.centrality as centrality")
                .accept(row -> {
                    System.out.println(
                            row.getNumber("id").longValue() + " " +
                                    row.getNumber("centrality").doubleValue());
                    consumer.accept(
                            row.getNumber("id").longValue(),
                            row.getNumber("centrality").doubleValue());
                    return true;
                });

        verifyMock();
    }

    private void verifyMock() {
        verify(consumer, times(1)).accept(eq(centerNodeId), AdditionalMatchers.eq(1.0, 0.01));
        verify(consumer, times(10)).accept(anyLong(), AdditionalMatchers.eq(0.588, 0.01));
    }
}
