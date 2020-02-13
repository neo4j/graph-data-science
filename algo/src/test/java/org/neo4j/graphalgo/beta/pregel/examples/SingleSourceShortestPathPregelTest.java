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
package org.neo4j.graphalgo.beta.pregel.examples;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphdb.Label;

import static org.neo4j.graphalgo.beta.pregel.examples.ComputationTestUtil.assertLongValues;

class SingleSourceShortestPathPregelTest extends AlgoTestBase {

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

    private Graph graph;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(TEST_GRAPH);
        graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .build()
            .load(HugeGraphFactory.class);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @Test
    void runSSSP() {
        int batchSize = 10;
        int maxIterations = 10;

        PregelConfig config = new PregelConfig.Builder()
            .isAsynchronous(true)
            .build();

        Pregel pregelJob = Pregel.withDefaultNodeValues(
            graph,
            config,
            new SingleSourceShortestPathPregel(0),
            batchSize,
            AlgoBaseConfig.DEFAULT_CONCURRENCY,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        HugeDoubleArray nodeValues = pregelJob.run(maxIterations);

        assertLongValues(db, NODE_LABEL, ID_PROPERTY, graph, nodeValues,
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
