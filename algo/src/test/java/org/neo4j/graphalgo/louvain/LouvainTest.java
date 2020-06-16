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
package org.neo4j.graphalgo.louvain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunities;
import static org.neo4j.graphalgo.CommunityHelper.assertCommunitiesWithLabels;
import static org.neo4j.graphalgo.TestLog.INFO;
import static org.neo4j.graphalgo.TestSupport.assertMemoryEstimation;
import static org.neo4j.graphalgo.core.ProcedureConstants.TOLERANCE_DEFAULT;
import static org.neo4j.graphalgo.graphbuilder.TransactionTerminationTestUtils.assertTerminates;

class LouvainTest extends AlgoTestBase {

    static ImmutableLouvainStreamConfig.Builder defaultConfigBuilder() {
        return ImmutableLouvainStreamConfig.builder()
            .maxLevels(10)
            .maxIterations(10)
            .tolerance(TOLERANCE_DEFAULT)
            .includeIntermediateCommunities(true)
            .concurrency(1);
    }

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {seed: 1})" +        // 0
        ", (b:Node {seed: 1})" +        // 1
        ", (c:Node {seed: 1})" +        // 2
        ", (d:Node {seed: 1})" +        // 3
        ", (e:Node {seed: 1})" +        // 4
        ", (f:Node {seed: 1})" +        // 5
        ", (g:Node {seed: 2})" +        // 6
        ", (h:Node {seed: 2})" +        // 7
        ", (i:Node {seed: 2})" +        // 8
        ", (j:Node {seed: 42})" +       // 9
        ", (k:Node {seed: 42})" +       // 10
        ", (l:Node {seed: 42})" +       // 11
        ", (m:Node {seed: 42})" +       // 12
        ", (n:Node {seed: 42})" +       // 13
        ", (x:Node {seed: 1})" +        // 14
        ", (u:Some)" +
        ", (v:Other)" +
        ", (w:Label)" +

        ", (a)-[:TYPE {weight: 1.0}]->(b)" +
        ", (a)-[:TYPE {weight: 1.0}]->(d)" +
        ", (a)-[:TYPE {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE {weight: 1.0}]->(x)" +
        ", (b)-[:TYPE {weight: 1.0}]->(g)" +
        ", (b)-[:TYPE {weight: 1.0}]->(e)" +
        ", (c)-[:TYPE {weight: 1.0}]->(x)" +
        ", (c)-[:TYPE {weight: 1.0}]->(f)" +
        ", (d)-[:TYPE {weight: 1.0}]->(k)" +
        ", (e)-[:TYPE {weight: 1.0}]->(x)" +
        ", (e)-[:TYPE {weight: 0.01}]->(f)" +
        ", (e)-[:TYPE {weight: 1.0}]->(h)" +
        ", (f)-[:TYPE {weight: 1.0}]->(g)" +
        ", (g)-[:TYPE {weight: 1.0}]->(h)" +
        ", (h)-[:TYPE {weight: 1.0}]->(i)" +
        ", (h)-[:TYPE {weight: 1.0}]->(j)" +
        ", (i)-[:TYPE {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE {weight: 1.0}]->(m)" +
        ", (j)-[:TYPE {weight: 1.0}]->(n)" +
        ", (k)-[:TYPE {weight: 1.0}]->(m)" +
        ", (k)-[:TYPE {weight: 1.0}]->(l)" +
        ", (l)-[:TYPE {weight: 1.0}]->(n)" +
        ", (m)-[:TYPE {weight: 1.0}]->(n)";

    @Test
    void unweightedLouvain() {
        Graph graph = loadGraph(DB_CYPHER, false);

        Louvain algorithm = new Louvain(
            graph,
            defaultConfigBuilder().build(),
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        ).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        final HugeLongArray[] dendrogram = algorithm.dendrograms();
        final double[] modularities = algorithm.modularities();

        assertCommunities(
            dendrogram[0],
            new long[]{0, 1, 3},
            new long[]{2, 4, 5, 14},
            new long[]{6, 7, 8},
            new long[]{9, 10, 11, 12, 13}
        );

        assertCommunities(
            dendrogram[1],
            new long[]{0, 1, 2, 3, 4, 5, 14},
            new long[]{6, 7, 8},
            new long[]{9, 10, 11, 12, 13}
        );

        assertEquals(2, algorithm.levels());
        assertEquals(0.38, modularities[modularities.length - 1], 0.01);
    }

    @Test
    void weightedLouvain() {
        Graph graph = loadGraph(DB_CYPHER, true);

        Louvain algorithm = new Louvain(
            graph,
            defaultConfigBuilder().build(),
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        ).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        final HugeLongArray[] dendrogram = algorithm.dendrograms();
        final double[] modularities = algorithm.modularities();

        assertCommunities(
            dendrogram[0],
            new long[]{0, 1, 3},
            new long[]{2, 4, 14},
            new long[]{5, 6},
            new long[]{7, 8},
            new long[]{9, 10, 11, 12, 13}
        );

        assertCommunities(
            dendrogram[1],
            new long[]{0, 1, 2, 3, 4, 5, 6, 14},
            new long[]{7, 8, 9, 10, 11, 12, 13}
        );

        assertEquals(2, algorithm.levels());
        assertEquals(0.37, modularities[modularities.length - 1], 0.01);
    }

    @Test
    void seededLouvain() {
        Graph graph = loadGraph(DB_CYPHER, true, "seed");

        Louvain algorithm = new Louvain(
            graph,
            defaultConfigBuilder().seedProperty("seed").build(),
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        ).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        final HugeLongArray[] dendrogram = algorithm.dendrograms();
        final double[] modularities = algorithm.modularities();

        Map<Long, Long[]> expectedCommunitiesWithlabels = MapUtil.genericMap(
            new HashMap<>(),
            1L, new Long[]{0L, 1L, 2L, 3L, 4L, 5L, 14L},
            2L, new Long[]{6L, 7L, 8L},
            42L, new Long[]{9L, 10L, 11L, 12L, 13L}
        );

        assertCommunitiesWithLabels(
            dendrogram[0],
            expectedCommunitiesWithlabels
        );

        assertEquals(1, algorithm.levels());
        assertEquals(0.38, modularities[modularities.length - 1], 0.01);
    }

    @Test
    void testTolerance() {
        Graph graph = loadGraph(DB_CYPHER);

        Louvain algorithm = new Louvain(
            graph,
            ImmutableLouvainStreamConfig.builder()
                .maxLevels(10)
                .maxIterations(10)
                .tolerance(2.0)
                .includeIntermediateCommunities(false)
                .concurrency(1)
                .build(),
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        ).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        assertEquals(1, algorithm.levels());
    }

    @Test
    void testMaxLevels() {
        Graph graph = loadGraph(DB_CYPHER);

        Louvain algorithm = new Louvain(
            graph,
            ImmutableLouvainStreamConfig.builder()
                .maxLevels(1)
                .maxIterations(10)
                .tolerance(TOLERANCE_DEFAULT)
                .includeIntermediateCommunities(false)
                .concurrency(1)
                .build(),
            Pools.DEFAULT,
            progressLogger,
            AllocationTracker.EMPTY
        ).withTerminationFlag(TerminationFlag.RUNNING_TRUE);

        algorithm.compute();

        assertEquals(1, algorithm.levels());
    }

    @ParameterizedTest
    @MethodSource("memoryEstimationTuples")
    void testMemoryEstimation(int concurrency, int levels, long expectedMinBytes, long expectedMaxBytes) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000L)
            .maxRelCount(500_000L)
            .build();

        LouvainStreamConfig config = ImmutableLouvainStreamConfig.builder()
            .maxLevels(levels)
            .maxIterations(10)
            .tolerance(TOLERANCE_DEFAULT)
            .includeIntermediateCommunities(false)
            .concurrency(1)
            .build();

        assertMemoryEstimation(
            () -> new LouvainFactory<>().memoryEstimation(config),
            dimensions,
            concurrency,
            expectedMinBytes,
            expectedMaxBytes
        );
    }

    @Test
    void testMemoryEstimationUsesOnlyOnePropertyForEachEntity() {
        ImmutableGraphDimensions.Builder dimensionsBuilder = ImmutableGraphDimensions.builder()
            .nodeCount(100_000L)
            .maxRelCount(500_000L);

        GraphDimensions dimensionsWithoutProperties = dimensionsBuilder.build();
        GraphDimensions dimensionsWithOneProperty = dimensionsBuilder
            .putRelationshipPropertyToken("foo", 1)
            .build();
        GraphDimensions dimensionsWithTwoProperties = dimensionsBuilder
            .putRelationshipPropertyToken("foo", 1)
            .putRelationshipPropertyToken("bar", 1)
            .build();

        LouvainStreamConfig config = ImmutableLouvainStreamConfig.builder()
            .maxLevels(1)
            .maxIterations(10)
            .tolerance(TOLERANCE_DEFAULT)
            .includeIntermediateCommunities(false)
            .concurrency(1)
            .build();

        MemoryTree memoryTree = new LouvainFactory<>().memoryEstimation(config).estimate(dimensionsWithoutProperties, 1);
        MemoryTree memoryTreeOneProperty = new LouvainFactory<>().memoryEstimation(config).estimate(dimensionsWithOneProperty, 1);
        MemoryTree memoryTreeTwoProperties = new LouvainFactory<>().memoryEstimation(config).estimate(dimensionsWithTwoProperties, 1);

        assertEquals(memoryTree.memoryUsage(), memoryTreeOneProperty.memoryUsage());
        assertEquals(memoryTreeOneProperty.memoryUsage(), memoryTreeTwoProperties.memoryUsage());
    }

    @Test
    void testCanBeInterruptedByTxCancelation() {
        Graph graph = new RandomGraphGenerator(
            100_000,
            10,
            RelationshipDistribution.UNIFORM,
            Optional.empty(),
            AllocationTracker.EMPTY
        ).generate();

        assertTerminates((terminationFlag) ->
            new Louvain(
                graph,
                defaultConfigBuilder().concurrency(2).build(),
                Pools.DEFAULT,
                progressLogger,
                AllocationTracker.EMPTY
            )
            .withTerminationFlag(terminationFlag)
            .compute(), 500, 1000
        );
    }

    @Test
    void testLogging() {
        var graph = loadGraph(DB_CYPHER);

        var config = defaultConfigBuilder().build();

        var testLogger = new TestProgressLogger(0, "Louvain", config.concurrency());

        var louvain = new Louvain(
            graph,
            config,
            Pools.DEFAULT,
            testLogger,
            AllocationTracker.EMPTY
        );

        louvain.compute();

        assertTrue(testLogger.containsMessage(INFO, ":: Start"));
        assertTrue(testLogger.containsMessage(INFO, "Level 1 :: Finished"));
        assertTrue(testLogger.containsMessage(INFO, ":: Finished"));
    }

    static Stream<Arguments> memoryEstimationTuples() {
        return Stream.of(
            arguments(1, 1, 6414145, 23941592),
            arguments(1, 10, 6414145, 31141952),
            arguments(4, 1, 6417433, 29745968),
            arguments(4, 10, 6417433, 36946328),
            arguments(42, 1, 6459081, 105719456),
            arguments(42, 10, 6459081, 112919816)
        );
    }

    private Graph loadGraph(String cypher, String... nodeProperties) {
        return loadGraph(cypher, false, nodeProperties);
    }

    private Graph loadGraph(
        String cypher,
        boolean loadRelationshipProperty,
        String... nodeProperties
    ) {
        runQuery(cypher);

        PropertyMapping[] nodePropertyMappings = Arrays.stream(nodeProperties)
            .map(p -> PropertyMapping.of(p, -1))
            .toArray(PropertyMapping[]::new);

        PropertyMapping relationshipPropertyMapping = PropertyMapping.of("weight", 1.0);

        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node")
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_OUT",
                RelationshipProjection.of("TYPE", Orientation.NATURAL)
            )
            .putRelationshipProjectionsWithIdentifier(
                "TYPE_IN",
                RelationshipProjection.of("TYPE", Orientation.REVERSE)
            )
            .addNodeProperties(nodePropertyMappings);
        if (loadRelationshipProperty) {
            storeLoaderBuilder.addRelationshipProperty(relationshipPropertyMapping);
        }
        return storeLoaderBuilder
            .build()
            .graph();

    }
}
