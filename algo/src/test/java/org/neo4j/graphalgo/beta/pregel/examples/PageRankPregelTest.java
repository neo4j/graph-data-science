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
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphdb.Label;

import static org.neo4j.graphalgo.beta.pregel.examples.ComputationTestUtil.assertDoubleValues;

class PageRankPregelTest extends AlgoTestBase {

    private static final String ID_PROPERTY = "id";

    private static final Label NODE_LABEL = Label.label("Node");

    // https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.jpg
    private static final String TEST_GRAPH =
            "CREATE" +
            "  (a:Node { id: 0, name: 'a' })" +
            ", (b:Node { id: 1, name: 'b' })" +
            ", (c:Node { id: 2, name: 'c' })" +
            ", (d:Node { id: 3, name: 'd' })" +
            ", (e:Node { id: 4, name: 'e' })" +
            ", (f:Node { id: 5, name: 'f' })" +
            ", (g:Node { id: 6, name: 'g' })" +
            ", (h:Node { id: 7, name: 'h' })" +
            ", (i:Node { id: 8, name: 'i' })" +
            ", (j:Node { id: 9, name: 'j' })" +
            ", (k:Node { id: 10, name: 'k' })" +
            ", (b)-[:REL]->(c)" +
            ", (c)-[:REL]->(b)" +
            ", (d)-[:REL]->(a)" +
            ", (d)-[:REL]->(b)" +
            ", (e)-[:REL]->(b)" +
            ", (e)-[:REL]->(d)" +
            ", (e)-[:REL]->(f)" +
            ", (f)-[:REL]->(b)" +
            ", (f)-[:REL]->(e)" +
            ", (g)-[:REL]->(b)" +
            ", (g)-[:REL]->(e)" +
            ", (h)-[:REL]->(b)" +
            ", (h)-[:REL]->(e)" +
            ", (i)-[:REL]->(b)" +
            ", (i)-[:REL]->(e)" +
            ", (j)-[:REL]->(e)" +
            ", (k)-[:REL]->(e)";

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
            .load(NativeFactory.class);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @Test
    void runPR() {
        int batchSize = 10;
        int maxIterations = 10;
        float dampingFactor = 0.85f;

        PregelConfig config = new PregelConfig.Builder()
            .withInitialNodeValue(1.0 / graph.nodeCount())
            .isAsynchronous(false)
            .build();

        Pregel pregelJob = Pregel.withDefaultNodeValues(
            graph,
            config,
            new PageRankPregel(graph.nodeCount(), dampingFactor),
            batchSize,
            AlgoBaseConfig.DEFAULT_CONCURRENCY,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        final HugeDoubleArray nodeValues = pregelJob.run(maxIterations);

        assertDoubleValues(db, NODE_LABEL, ID_PROPERTY, graph, nodeValues, 1e-3,
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
