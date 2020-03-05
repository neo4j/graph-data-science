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
package gds.training;

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
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class K1ColoringTest extends AlgoTestBase {

    private static final String DB_CYPHER =
        "CREATE" +
        " (a)" +
        ",(c)" +
        ",(b)" +
        ",(d)" +
        ",(a)-[:REL]->(b)" +
        ",(b)-[:REL]->(d)" +
        ",(a)-[:REL]->(d)" +
        ",(d)-[:REL]->(c)" +
        ",(a)-[:REL]->(c)";

    // a = 0
    // b = 1
    // c = 1
    // d = 2

    private Graph graph;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);

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
    void runK1Coloring() {
        int batchSize = 10;
        int maxIterations = 10;

        PregelConfig config = new PregelConfig.Builder()
            .isAsynchronous(true)
            .build();

        Pregel pregelJob = Pregel.withDefaultNodeValues(
            graph,
            config,
            new K1ColoringExample(),
            batchSize,
            AlgoBaseConfig.DEFAULT_CONCURRENCY,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        HugeDoubleArray nodeValues = pregelJob.run(maxIterations);

        assertAll(() -> {
            assertEquals(0, nodeValues.get(0), "nodeId = 0");
            assertEquals(1, nodeValues.get(1), "nodeId = 1");
            assertEquals(1, nodeValues.get(2), "nodeId = 2");
            assertEquals(2, nodeValues.get(3), "nodeId = 3");
        });
    }
}
