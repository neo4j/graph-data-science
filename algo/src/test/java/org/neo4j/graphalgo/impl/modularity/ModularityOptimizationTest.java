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

package org.neo4j.graphalgo.impl.modularity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunities;

class ModularityOptimizationTest {

    GraphDatabaseAPI db;

    static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a', seed1: 0, seed2: 1})" +
        ", (b:Node {name:'b', seed1: 0, seed2: 1})" +
        ", (c:Node {name:'c', seed1: 2, seed2: 42})" +
        ", (d:Node {name:'d', seed1: 2, seed2: 42})" +
        ", (e:Node {name:'e', seed1: 2, seed2: 42})" +
        ", (f:Node {name:'f', seed1: 2, seed2: 42})" +
        ", (a)-[:TYPE {weight: 0.01}]->(b)" +
        ", (a)-[:TYPE {weight: 5.0}]->(e)" +
        ", (a)-[:TYPE {weight: 5.0}]->(f)" +
        ", (b)-[:TYPE {weight: 5.0}]->(c)" +
        ", (b)-[:TYPE {weight: 5.0}]->(d)" +
        ", (c)-[:TYPE {weight: 0.01}]->(e)" +
        ", (f)-[:TYPE {weight: 0.01}]->(d)";

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void shutdownGraphDb() {
        if (db != null) db.shutdown();
    }

    @Test
    void testUnweighted() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            3,
            null,
            3,
            2,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
        );

        pmo.compute();

        assertEquals(0.12244, pmo.getModularity(), 0.001);
        assertCommunities(getCommunityIds(graph.nodeCount(), pmo), new long[]{0, 1, 2, 4}, new long[]{3, 5});
        assertTrue(pmo.getIterations() <= 3);
    }

    @Test
    void testWeighted() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withRelationshipProperties(
                PropertyMapping.of("weight", 1.0)
            )
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            3,
            null,
            3,
            2,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
        );

        pmo.compute();

        assertEquals(0.4985, pmo.getModularity(), 0.001);
        assertCommunities(getCommunityIds(graph.nodeCount(), pmo), new long[]{0, 4, 5} , new long[]{1, 2, 3});
        assertTrue(pmo.getIterations() <= 3);
    }

    @Test
    void testSeedingWithBiggerSeedValues() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withOptionalNodeProperties(
                PropertyMapping.of("seed2", -1)
            )
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            10,
            graph.nodeProperties("seed2"),
            1,
            100,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
        );

        pmo.compute();

        long[] actualCommunities = getCommunityIds(graph.nodeCount(), pmo);
        assertEquals(-0.0816, pmo.getModularity(), 0.001);
        assertCommunities(actualCommunities, new long[]{0, 1}, new long[]{2, 3, 4, 5});
        assertTrue(actualCommunities[0] == 1 && actualCommunities[2] == 42);
        assertTrue(pmo.getIterations() <= 3);
    }

    @Test
    void testSeeding() {
        Graph graph = new GraphLoader(db)
            .withAnyLabel()
            .withAnyRelationshipType()
            .withOptionalNodeProperties(
                PropertyMapping.of("seed1", -1)
            )
            .withDirection(Direction.BOTH)
            .load(HugeGraphFactory.class);

        ModularityOptimization pmo = new ModularityOptimization(
            graph,
            Direction.BOTH,
            10,
            graph.nodeProperties("seed1"),
            1,
            100,
            Pools.DEFAULT,
            AllocationTracker.EMPTY,
            NullLog.getInstance()
        );

        pmo.compute();

        long[] actualCommunities = getCommunityIds(graph.nodeCount(), pmo);
        assertEquals(-0.0816, pmo.getModularity(), 0.001);
        assertCommunities(actualCommunities, new long[]{0, 1}, new long[]{2, 3, 4, 5});
        assertTrue(actualCommunities[0] == 0 && actualCommunities[2] == 2);
        assertTrue(pmo.getIterations() <= 3);
    }

    private long[] getCommunityIds(long nodeCount, ModularityOptimization pmo) {
        long[] communityIds = new long[(int)nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            communityIds[i] = pmo.getCommunityId(i);
        }
        return communityIds;
    }
}