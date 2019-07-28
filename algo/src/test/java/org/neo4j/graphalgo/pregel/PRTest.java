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
import org.neo4j.test.rule.ImpermanentDatabaseRule;

public class PRTest {

    private static final String MESSAGE_PROPERTY = "message";

    // https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.jpg
    private static final String TEST_GRAPH = "" +
                                             "CREATE (a:Node { name: 'a' })\n" +
                                             "CREATE (b:Node { name: 'b' })\n" +
                                             "CREATE (c:Node { name: 'c' })\n" +
                                             "CREATE (d:Node { name: 'd' })\n" +
                                             "CREATE (e:Node { name: 'e' })\n" +
                                             "CREATE (f:Node { name: 'f' })\n" +
                                             "CREATE (g:Node { name: 'g' })\n" +
                                             "CREATE (h:Node { name: 'h' })\n" +
                                             "CREATE (i:Node { name: 'i' })\n" +
                                             "CREATE (j:Node { name: 'j' })\n" +
                                             "CREATE (k:Node { name: 'k' })\n" +
                                             "CREATE\n" +
                                             "  (b)-[:REL { message: 1.0 }]->(c),\n" +
                                             "  (c)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (d)-[:REL { message: 1.0 }]->(a),\n" +
                                             "  (d)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (e)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (e)-[:REL { message: 1.0 }]->(d),\n" +
                                             "  (e)-[:REL { message: 1.0 }]->(f),\n" +
                                             "  (f)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (f)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (g)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (g)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (h)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (h)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (i)-[:REL { message: 1.0 }]->(b),\n" +
                                             "  (i)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (j)-[:REL { message: 1.0 }]->(e),\n" +
                                             "  (k)-[:REL { message: 1.0 }]->(e)\n";


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
                .withOptionalRelationshipWeightsFromProperty(MESSAGE_PROPERTY, 1.0)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void runPR() {

        int batchSize = 10;
        int maxIterations = 20;
        float jumpProbability = 0.15f;
        float dampingFactor = 0.85f;

        Pregel pregelJob = new Pregel(
                graph,
                new PRComputation(graph.nodeCount(), jumpProbability, dampingFactor),
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                ProgressLogger.NULL_LOGGER);

        final HugeWeightMapping nodeValues = pregelJob.run(maxIterations);

        System.out.printf("Ran %d iterations.%n", pregelJob.getIterations());

        for (int i = 0; i < graph.nodeCount(); i++) {
            System.out.println(String.format("nodeId: %d, rank: %.4f", i, nodeValues.nodeWeight(i)));
        }
    }
}
