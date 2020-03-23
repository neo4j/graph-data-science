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
package org.neo4j.graphalgo.beta.k1coloring;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.CypherLoaderBuilder;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.huge.UnionGraph;
import org.neo4j.graphalgo.core.loading.CypherFactory;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig.AllowSelfLoops;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.AllGraphTypesTest;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.applyInTransaction;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;

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
    void testK1Coloring(Class<? extends GraphStoreFactory> graphImpl) {
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

        GraphLoader graphLoader;
        if (graphImpl == CypherFactory.class) {
            graphLoader = new CypherLoaderBuilder()
                .api(db)
                .graphName("cypher")
                .nodeQuery(ALL_NODES_QUERY)
                .relationshipQuery(ALL_RELATIONSHIPS_QUERY)
                .build();
        } else {
            graphLoader = new StoreLoaderBuilder()
                .api(db)
                .loadAnyLabel()
                .loadAnyRelationshipType()
                .build();
        }

        graph = applyInTransaction(db, tx -> graphLoader.load(graphImpl));

        K1Coloring k1Coloring = new K1Coloring(
            graph,
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
        long seed = 42L;

        RandomGraphGenerator outGenerator = new RandomGraphGenerator(
            100_000,
            5,
            RelationshipDistribution.POWER_LAW,
            seed,
            Optional.empty(),
            AllocationTracker.EMPTY
        );

        RandomGraphGenerator inGenerator = new RandomGraphGenerator(
            100_000,
            5,
            RelationshipDistribution.POWER_LAW,
            seed,
            Optional.empty(),
            Aggregation.NONE, Orientation.REVERSE, AllowSelfLoops.NO, AllocationTracker.EMPTY
        );

        Graph naturalGraph = outGenerator.generate();
        Graph reverseGraph = inGenerator.generate();
        Graph graph = UnionGraph.of(Arrays.asList(naturalGraph, reverseGraph));

        K1Coloring k1Coloring = new K1Coloring(
            graph,
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
            graph.forEachRelationship(nodeId, (source, target) -> {
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

        assertMemoryEstimation(nodeCount, concurrency, 825256);
    }

    @Test
    void shouldComputeMemoryEstimation4Threads() {
        long nodeCount = 100_000L;
        int concurrency = 4;
        assertMemoryEstimation(nodeCount, concurrency, 863032);
    }

    @Test
    void shouldComputeMemoryEstimation42Threads() {
        long nodeCount = 100_000L;
        int concurrency = 42;
        assertMemoryEstimation(nodeCount, concurrency, 1341528);
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
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        K1ColoringStreamConfig config = ImmutableK1ColoringStreamConfig.builder().build();
        final MemoryRange actual = new K1ColoringFactory<>()
            .memoryEstimation(config)
            .estimate(dimensions, concurrency)
            .memoryUsage();

        assertEquals(actual.min, actual.max);
        assertEquals(expected, actual.min);
    }

}



