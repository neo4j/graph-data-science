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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.pregel.pagerank.PRComputation;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.neo4j.graphalgo.pregel.ComputationTestUtil.*;

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
                .withDirection(Direction.OUTGOING)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void runPR() {

        int batchSize = 10;
        int maxIterations = 10;
        float dampingFactor = 0.85f;

        Pregel pregelJob = Pregel.withDefaultNodeValues(
                graph,
                () -> new PRComputation(graph.nodeCount(), dampingFactor),
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                ProgressLogger.NULL_LOGGER);

        final HugeDoubleArray nodeValues = pregelJob.run(maxIterations);

        assertDoubleValues(DB, NODE_LABEL, ID_PROPERTY, graph, nodeValues, 1e-3,
                0.0276, // a
                0.3483, // b
                0.2650, // c
                0.0330, // d
                0.0682, // e
                0.0330, // f
                0.0136, // g
                0.0136, // h
                0.0136, // i
                0.0136, // j
                0.0136 // k
        );
    }
}
