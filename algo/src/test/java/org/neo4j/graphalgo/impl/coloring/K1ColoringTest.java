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
package org.neo4j.graphalgo.impl.coloring;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.impl.generator.RelationshipDistribution;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import static org.neo4j.graphalgo.core.utils.ParallelUtil.DEFAULT_BATCH_SIZE;
import static org.neo4j.graphalgo.core.utils.ParallelUtil.canRunInParallel;

class K1ColoringTest extends AlgoTestBase {

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @AfterEach
    void shutdownGraphDb() {
        if (db != null) db.shutdown();
    }

    @AllGraphTypesTest
    void testK1Coloring(Class<? extends GraphFactory> graphImpl) {
        final String DB_CYPHER =
            "CREATE" +
            " (a)" +
            ",(b)" +
            ",(c)" +
            ",(d)" +
            ",(a)-[:REL]->(b)" +
            ",(a)-[:REL]->(c)";

        runQuery(DB_CYPHER);

        Graph graph;

        GraphLoader graphLoader = new GraphLoader(db, Pools.DEFAULT)
            .withDirection(Direction.OUTGOING)
            .withDefaultConcurrency();


        if (graphImpl == CypherGraphFactory.class) {
            graphLoader
                .withLabel("MATCH (n) RETURN id(n) AS id")
                .withRelationshipType("MATCH (m)-->(n) \n" +
                                      "RETURN id(m) AS source, id(n) AS target")
                .withName("cypher");
        }

        graph = QueryRunner.runInTransaction(db, () -> graphLoader.load(graphImpl));

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            Direction.OUTGOING,
            1000,
            DEFAULT_BATCH_SIZE,
            1,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        k1Coloring.compute();

        HugeLongArray colors = k1Coloring.colors();

        assertNotEquals(colors.get(0), colors.get(1));
        assertNotEquals(colors.get(0), colors.get(2));
        assertEquals(colors.get(1), colors.get(2));
    }

    @Test
    void testParallelK1Coloring() {
        RandomGraphGenerator generator = new RandomGraphGenerator(
            100_000,
            5,
            RelationshipDistribution.POWER_LAW,
            Optional.empty(),
            AllocationTracker.EMPTY
        );

        Graph graph = generator.generate();

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            Direction.BOTH,
            100,
            DEFAULT_BATCH_SIZE,
            8,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        k1Coloring.compute();
        HugeLongArray colors = k1Coloring.colors();

        Set<Long> colorsUsed = new HashSet<>(100);
        MutableLong conflicts = new MutableLong(0);
        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, Direction.BOTH, (source, target) -> {
                if (source != target && colors.get(source) == colors.get(target)) {
                    conflicts.increment();
                }
                colorsUsed.add(colors.get(source));
                return true;
            });
            return true;
        });

        assertTrue(conflicts.getValue() < 20);
        assertTrue(colorsUsed.size() < 20);
    }


    @Test
    void shouldComputeMemoryEstimation1Thread() {
        long nodeCount = 100_000L;
        int concurrency = 1;

        assertMemoryEstimation(nodeCount, concurrency, 825272);
    }

    @Test
    void shouldComputeMemoryEstimation4Threads() {
        long nodeCount = 100_000L;
        int concurrency = 4;
        assertMemoryEstimation(nodeCount, concurrency, 863072);
    }

    @Test
    void shouldComputeMemoryEstimation42Threads() {
        long nodeCount = 100_000L;
        int concurrency = 42;
        assertMemoryEstimation(nodeCount, concurrency, 1341872);
    }

    @Test
    void everyNodeShouldHaveBeenColored() {
        RandomGraphGenerator generator = new RandomGraphGenerator(
            100_000,
            10,
            RelationshipDistribution.UNIFORM,
            Optional.empty(),
            AllocationTracker.EMPTY
        );

        Graph graph = generator.generate();

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            Direction.BOTH,
            100,
            DEFAULT_BATCH_SIZE,
            8,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        k1Coloring.compute();

        assertFalse(k1Coloring.usedColors().get(ColoringStep.INITIAL_FORBIDDEN_COLORS));
    }

    private void assertMemoryEstimation(long nodeCount, int concurrency, long expected) {
        GraphDimensions dimensions = new GraphDimensions.Builder().setNodeCount(nodeCount).build();
        final MemoryRange actual = new K1ColoringFactory()
            .memoryEstimation(ProcedureConfiguration.empty())
            .estimate(dimensions, concurrency)
            .memoryUsage();

        assertEquals(actual.min, actual.max);
        assertEquals(expected, actual.min);
    }

}



