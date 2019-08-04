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
package org.neo4j.graphalgo.pregel;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.pregel.pagerank.PRComputation;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.HashMap;
import java.util.Map;

public class PRTest {

    private static final String ID_PROPERTY = "id";

    private static final Label NODE_LABEL = Label.label("Node");

    // https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.jpg
    private static final String TEST_GRAPH = "" +
                                             "CREATE (a:Node { id: 0, name: 'a' })\n" +
                                             "CREATE (b:Node { id: 1, name: 'b' })\n" +
                                             "CREATE (c:Node { id: 2, name: 'c' })\n" +
                                             "CREATE (d:Node { id: 3, name: 'd' })\n" +
                                             "CREATE (e:Node { id: 4, name: 'e' })\n" +
                                             "CREATE (f:Node { id: 5, name: 'f' })\n" +
                                             "CREATE (g:Node { id: 6, name: 'g' })\n" +
                                             "CREATE (h:Node { id: 7, name: 'h' })\n" +
                                             "CREATE (i:Node { id: 8, name: 'i' })\n" +
                                             "CREATE (j:Node { id: 9, name: 'j' })\n" +
                                             "CREATE (k:Node { id: 10, name: 'k' })\n" +
                                             "CREATE\n" +
                                             "  (b)-[:REL]->(c),\n" +
                                             "  (c)-[:REL]->(b),\n" +
                                             "  (d)-[:REL]->(a),\n" +
                                             "  (d)-[:REL]->(b),\n" +
                                             "  (e)-[:REL]->(b),\n" +
                                             "  (e)-[:REL]->(d),\n" +
                                             "  (e)-[:REL]->(f),\n" +
                                             "  (f)-[:REL]->(b),\n" +
                                             "  (f)-[:REL]->(e),\n" +
                                             "  (g)-[:REL]->(b),\n" +
                                             "  (g)-[:REL]->(e),\n" +
                                             "  (h)-[:REL]->(b),\n" +
                                             "  (h)-[:REL]->(e),\n" +
                                             "  (i)-[:REL]->(b),\n" +
                                             "  (i)-[:REL]->(e),\n" +
                                             "  (j)-[:REL]->(e),\n" +
                                             "  (k)-[:REL]->(e)\n";


    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setup() {
        DB.execute(TEST_GRAPH);
    }

    @AfterClass
    public static void shutdown() {
        DB.shutdown();
    }

    private Graph graph;

    public PRTest() {
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                // The following options need to be default for Pregel
                .withDirection(Direction.BOTH)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void runPR() {

        int batchSize = 10;
        int maxIterations = 40;
        float jumpProbability = 0.15f;
        float dampingFactor = 0.85f;

        Pregel pregelJob = Pregel.withDefaultNodeValues(
                graph,
                new PRComputation(graph.nodeCount(), jumpProbability, dampingFactor),
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                ProgressLogger.NULL_LOGGER);

        final HugeWeightMapping nodeValues = pregelJob.run(maxIterations);

        assertValues(graph, nodeValues,
                0, 0.0276,
                1, 0.3138,
                2, 0.2832,
                3, 0.0330,
                4, 0.0682,
                5, 0.0330,
                6, 0.0136,
                7, 0.0136,
                8, 0.0136,
                9, 0.0136,
                10, 0.0136);
    }

    private void assertValues(final Graph graph, HugeWeightMapping computedValues, final double... values) {
        Map<Long, Double> expectedValues = new HashMap<>();
        try (Transaction tx = DB.beginTx()) {
            for (int i = 0; i < values.length; i += 2) {
                expectedValues.put(DB.findNode(NODE_LABEL, ID_PROPERTY, (long) values[i]).getId(), values[i + 1]);
            }
            tx.success();
        }

        expectedValues.forEach((idProp, expectedValue) -> {
            long neoId = graph.toOriginalNodeId(idProp);
            double computedValue = computedValues.nodeWeight(neoId);
            Assert.assertEquals(
                    String.format("Node.id = %d should have page rank %f", idProp, expectedValue),
                    expectedValue,
                    computedValue,
                    1e-3);
        });
    }
}
