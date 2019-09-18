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
import org.neo4j.graphalgo.pregel.paths.SSSPComputation;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.neo4j.graphalgo.pregel.ComputationTestUtil.*;

public class SSSPTest {

    private static final String ID_PROPERTY = "id";

    private static final Label NODE_LABEL = Label.label("Node");

    private static final String TEST_GRAPH =
            "CREATE" +
            "  (nA:Node { id: 0 })" +
            ", (nB:Node { id: 1 })" +
            ", (nC:Node { id: 2 })" +
            ", (nD:Node { id: 3 })" +
            ", (nE:Node { id: 4 })" +
            ", (nF:Node { id: 5 })" +
            ", (nG:Node { id: 6 })" +
            ", (nH:Node { id: 7 })" +
            ", (nI:Node { id: 8 })" +
            // {J}
            ", (nJ:Node { id: 9 })" +
            // {A, B, C, D}
            ", (nA)-[:TYPE]->(nB)" +
            ", (nB)-[:TYPE]->(nC)" +
            ", (nC)-[:TYPE]->(nD)" +
            ", (nA)-[:TYPE]->(nC)" +
            // {E, F, G}
            ", (nE)-[:TYPE]->(nF)" +
            ", (nF)-[:TYPE]->(nG)" +
            // {H, I}
            ", (nI)-[:TYPE]->(nH)";

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

    private final Graph graph;

    public SSSPTest() {
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withDirection(Direction.BOTH)
                .load(HugeGraphFactory.class);
    }

    @Test
    public void runSSSP() {
        int batchSize = 10;
        int maxIterations = 10;

        Pregel pregelJob = Pregel.withDefaultNodeValues(
                graph,
                () -> new SSSPComputation(0),
                batchSize,
                Pools.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY,
                ProgressLogger.NULL_LOGGER);

        HugeDoubleArray nodeValues = pregelJob.run(maxIterations);

        assertLongValues(DB, NODE_LABEL, ID_PROPERTY, graph, nodeValues,
                0,
                1,
                1,
                2,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE);
    }
}
