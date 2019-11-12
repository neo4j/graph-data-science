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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.graphbuilder.DefaultBuilder;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.betweenness.BetweennessCentrality;
import org.neo4j.graphalgo.impl.betweenness.MaxDepthBetweennessCentrality;
import org.neo4j.graphalgo.impl.betweenness.ParallelBetweennessCentrality;
import org.neo4j.graphalgo.impl.betweenness.RABrandesBetweennessCentrality;
import org.neo4j.graphalgo.impl.betweenness.RandomSelectionStrategy;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled("approximations")
public class BetweennessComparisionTest {

    private static GraphDatabaseAPI DB;

    public static final String TYPE = "TYPE";
    private static final int NODE_COUNT = 500;
    private static Graph graph;
    private static int centerId;
    private static double[] expected;

    @BeforeAll
    static void setupGraph() {
        DB = TestDatabaseCreator.createTestDatabase();
        long cId;
        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("build took " + l + "ms"))) {

            final DefaultBuilder defaultBuilder = GraphBuilder.create(DB)
                    .setLabel("Node")
                    .setRelationship(TYPE);

            final Node center = defaultBuilder.createNode();

            defaultBuilder
                    .newCompleteGraphBuilder()
                    .createCompleteGraph(NODE_COUNT - 1)
                    .forEachRelInTx(r -> {
                        if (Math.random() > 0.005) { // keep ~2.5 connections per node
                            r.delete();
                        }
                    })
                    .forEachNodeInTx(n -> {
                        defaultBuilder.createRelationship(n, center);
                    });

            cId = center.getId();
        }

        graph = new GraphLoader(DB)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withDirection(Direction.OUTGOING)
                .load(HugeGraphFactory.class);

        centerId = Math.toIntExact(graph.toMappedNodeId(cId));

        try (ProgressTimer start = ProgressTimer.start(l -> System.out.println("reference bc took " + l + "ms"))) {
            expected = new BetweennessCentrality(graph)
                    .compute()
                    .getCentrality();
        };
    }

    @Test
    void testMaxDepth() {
        for (int d = 2; d < 5; d++) {
            final double[] centrality = new MaxDepthBetweennessCentrality(graph, d)
                    .compute()
                    .getCentrality();
            assertLowError(centrality);
        }
    }

    @Test
    void test() {

        final double[] centrality = new RABrandesBetweennessCentrality(graph, Pools.DEFAULT, 1,
                new RandomSelectionStrategy(graph, 0.5))
                .compute()
                .getCentrality()
                .toArray();

        assertLowError(centrality);
    }


    @Test
    void testParallel() {

        final double[] centrality = new ParallelBetweennessCentrality(graph, Pools.DEFAULT, 4)
                .compute()
                .getCentrality()
                .toArray();

        assertLowError(centrality);
    }

    private void assertLowError(double[] centrality) {
        final Error e = new Error(centrality);
        System.out.println("meanError = " + e.mean());
        System.out.println("maxError = " + e.max());
        System.out.println("centerError = " + e.error(centerId));
        assertEquals(0.0, e.mean(), 0.2, "mean error exceeds 20%");
        assertEquals(0.0, e.error(centerId), 0.1, "error at most important node exceeded 10%");
    }

    private class Error {

        double[] absError = new double[NODE_COUNT];
        double fMax = 0;
        double fMin = Double.POSITIVE_INFINITY;
        double sum = 0.0;


        Error(double[] centrality) {
            for (int i = 0; i < NODE_COUNT; i++) {
                absError[i] = Math.abs(expected[i] - centrality[i]);
                fMin = Math.min(fMin, absError[i]);
                fMax = Math.max(fMax, absError[i]);
                sum += absError[i];
            }
        }

        double mean() {
            return sum > 0.0 ? (sum / (fMax - fMin)) * (1.0 / NODE_COUNT) : 0.0;
        }

        double error(int nodeId) {
            return absError[nodeId] > 0.0 ? (absError[nodeId] / (fMax - fMin)) : 0.0;
        }

        public double max() {
            return fMax > 0.0 ? (fMax / (fMax - fMin)) : 0.0;
        }
    }
}
