/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.louvain;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Aggregation;
import org.neo4j.gds.CommunityAlgorithmTasks;
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestGraph;
import org.neo4j.gds.TestProgressTrackerHelper;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.modularity.ModularityCalculator;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.Optional;
import java.util.function.LongUnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.core.ProcedureConstants.TOLERANCE_DEFAULT;
import static org.neo4j.gds.graphbuilder.TransactionTerminationTestUtils.assertTerminates;

@GdlExtension
class LouvainTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 0)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {seed: 1,seed2: -1})" +        // 0
        ", (b:Node {seed: 1,seed2: 10})" +        // 1
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

        ", (a)-[:TYPE_OUT {weight: 1.0}]->(b)" +
        ", (a)-[:TYPE_OUT {weight: 1.0}]->(d)" +
        ", (a)-[:TYPE_OUT {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE_OUT {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE_OUT {weight: 1.0}]->(x)" +
        ", (b)-[:TYPE_OUT {weight: 1.0}]->(g)" +
        ", (b)-[:TYPE_OUT {weight: 1.0}]->(e)" +
        ", (c)-[:TYPE_OUT {weight: 1.0}]->(x)" +
        ", (c)-[:TYPE_OUT {weight: 1.0}]->(f)" +
        ", (d)-[:TYPE_OUT {weight: 1.0}]->(k)" +
        ", (e)-[:TYPE_OUT {weight: 1.0}]->(x)" +
        ", (e)-[:TYPE_OUT {weight: 0.01}]->(f)" +
        ", (e)-[:TYPE_OUT {weight: 1.0}]->(h)" +
        ", (f)-[:TYPE_OUT {weight: 1.0}]->(g)" +
        ", (g)-[:TYPE_OUT {weight: 1.0}]->(h)" +
        ", (h)-[:TYPE_OUT {weight: 1.0}]->(i)" +
        ", (h)-[:TYPE_OUT {weight: 1.0}]->(j)" +
        ", (i)-[:TYPE_OUT {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE_OUT {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE_OUT {weight: 1.0}]->(m)" +
        ", (j)-[:TYPE_OUT {weight: 1.0}]->(n)" +
        ", (k)-[:TYPE_OUT {weight: 1.0}]->(m)" +
        ", (k)-[:TYPE_OUT {weight: 1.0}]->(l)" +
        ", (l)-[:TYPE_OUT {weight: 1.0}]->(n)" +
        ", (m)-[:TYPE_OUT {weight: 1.0}]->(n)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void testUnweighted() {
        var graph = graphStore.getGraph(
            NodeLabel.listOf("Node"),
            RelationshipType.listOf("TYPE_OUT"),
            Optional.empty()
        );
        IdFunction mappedId = name -> graph.toMappedNodeId(idFunction.of(name));

        var algorithm = new Louvain(
            graph,
            new Concurrency(1),
            LouvainParameters.DEFAULT_ITERATIONS,
            TOLERANCE_DEFAULT,
            LouvainParameters.DEFAULT_LEVELS,
            true,
            null,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );

        var result = algorithm.compute();

        final HugeLongArray[] dendrogram = result.dendrogramManager().getAllDendrograms();
        final double[] modularities = result.modularities();

        CommunityHelper.assertCommunities(
            dendrogram[0].toArray(),
            mappedId.of("a", "b", "d"),
            mappedId.of("c", "e", "f", "x"),
            mappedId.of("g", "h", "i"),
            mappedId.of("j", "k", "l", "m", "n")
        );

        CommunityHelper.assertCommunities(
            dendrogram[1].toArray(),
            mappedId.of("a", "b", "c", "d", "e", "f", "x"),
            mappedId.of("g", "h", "i"),
            mappedId.of("j", "k", "l", "m", "n")
        );

        assertEquals(2, result.ranLevels());
        assertEquals(0.38, modularities[modularities.length - 1], 0.01);
    }

    @Test
    void testWeighted() {
        var graph = graphStore.getGraph(
            NodeLabel.listOf("Node"),
            RelationshipType.listOf("TYPE_OUT"),
            Optional.of("weight")
        );

        IdFunction mappedId = name -> graphStore.getGraph(NodeLabel.of("Node")).toMappedNodeId(idFunction.of(name));

        var algorithm = new Louvain(
            graph,
            new Concurrency(1),
            LouvainParameters.DEFAULT_ITERATIONS,
            TOLERANCE_DEFAULT,
            LouvainParameters.DEFAULT_LEVELS,
            true,
            null,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );

        var result = algorithm.compute();

        final HugeLongArray[] dendrogram = result.dendrogramManager().getAllDendrograms();
        final double[] modularities = result.modularities();

        CommunityHelper.assertCommunities(
            dendrogram[0].toArray(),
            mappedId.of("a", "b", "d"),
            mappedId.of("c", "e", "x"),
            mappedId.of("f", "g"),
            mappedId.of("h", "i"),
            mappedId.of("j", "k", "l", "m", "n")
        );

        CommunityHelper.assertCommunities(
            dendrogram[1].toArray(),
            mappedId.of("a", "b", "c", "d", "e", "f", "g", "x"),
            mappedId.of("h", "i", "j", "k", "l", "m", "n")
        );

        assertEquals(2, result.ranLevels());
        assertEquals(0.37, modularities[modularities.length - 1], 0.01);
    }

    @Test
    void testSeeded() {
        var graph = graphStore.getGraph(
            NodeLabel.listOf("Node"),
            RelationshipType.listOf("TYPE_OUT"),
            Optional.of("weight")
        );

        IdFunction mappedId = name -> graphStore.getGraph(NodeLabel.of("Node")).toMappedNodeId(idFunction.of(name));

        var algorithm = new Louvain(
            graph,
            new Concurrency(1),
            LouvainParameters.DEFAULT_ITERATIONS,
            TOLERANCE_DEFAULT,
            LouvainParameters.DEFAULT_LEVELS,
            true,
            "seed",
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );

        var result = algorithm.compute();

        final HugeLongArray[] dendrogram = result.dendrogramManager().getAllDendrograms();
        final double[] modularities = result.modularities();

        var expectedCommunitiesWithLabels = Map.of(
            1L, mappedId.of("a", "b", "c", "d", "e", "f", "x"),
            2L, mappedId.of("g", "h", "i"),
            42L, mappedId.of("j", "k", "l", "m", "n")
        );

        CommunityHelper.assertCommunitiesWithLabels(
            dendrogram[0],
            expectedCommunitiesWithLabels
        );

        assertEquals(1, result.ranLevels());
        assertEquals(0.38, modularities[modularities.length - 1], 0.01);
    }

    @Test
    void testTolerance() {
        var graph = graphStore.getGraph(
            NodeLabel.listOf("Node"),
            RelationshipType.listOf("TYPE_OUT"),
            Optional.empty()
        );

        var algorithm = new Louvain(
            graph,
            new Concurrency(1),
            LouvainParameters.DEFAULT_ITERATIONS,
            2.0,
            LouvainParameters.DEFAULT_LEVELS,
            false,
            null,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );

        var result = algorithm.compute();

        assertEquals(1, result.ranLevels());
    }

    @Test
    void testMaxLevels() {
        var graph = graphStore.getGraph(
            NodeLabel.listOf("Node"),
            RelationshipType.listOf("TYPE_OUT"),
            Optional.empty()
        );

        var algorithm = new Louvain(
            graph,
            new Concurrency(1),
            LouvainParameters.DEFAULT_ITERATIONS,
            TOLERANCE_DEFAULT,
            1,
            false,
            null,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );

        var result = algorithm.compute();
        assertEquals(1, result.ranLevels());
    }


    @Test
    void testCanBeInterruptedByTxCancellation() {
        HugeGraph graph = RandomGraphGenerator.builder()
            .nodeCount(100_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .build()
            .generate();

        assertTerminates((terminationFlag) ->
            {
                var louvain = new Louvain(
                    graph,
                    new Concurrency(2),
                    LouvainParameters.DEFAULT_ITERATIONS,
                    TOLERANCE_DEFAULT,
                    LouvainParameters.DEFAULT_LEVELS,
                    LouvainParameters.DEFAULT_INTERMEDIATE_COMMUNITIES,
                    null,
                    ProgressTracker.NULL_TRACKER,
                    DefaultPool.INSTANCE,
                    terminationFlag
                );

                louvain.compute();
            }, 500, 1000
        );
    }



    @Test
    void shouldThrowOnNegativeSeed() {
        var graph = graphStore.getGraph(
            NodeLabel.listOf("Node"),
            RelationshipType.listOf("TYPE_OUT"),
            Optional.of("weight")
        );

        var algorithm = new Louvain(
            graph,
            new Concurrency(4),
            LouvainParameters.DEFAULT_ITERATIONS,
            TOLERANCE_DEFAULT,
            LouvainParameters.DEFAULT_LEVELS,
            LouvainParameters.DEFAULT_INTERMEDIATE_COMMUNITIES,
            "seed2",
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );

        assertThatThrownBy(algorithm::compute).hasMessageContaining("non-negative");

    }

    @Test
    void shouldGiveSameResultWithCalculator() {
        var myGraph = RandomGraphGenerator
            .builder()
            .nodeCount(1_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .direction(Direction.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.YES)
            .aggregation(Aggregation.SINGLE)
            .seed(42)
            .build()
            .generate();

        var louvain = new Louvain(
            myGraph,
            new Concurrency(4),
            LouvainParameters.DEFAULT_ITERATIONS,
            TOLERANCE_DEFAULT,
            LouvainParameters.DEFAULT_LEVELS,
            LouvainParameters.DEFAULT_INTERMEDIATE_COMMUNITIES,
            null,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );

        var result = louvain.compute();
        assertThat(result.ranLevels()).isGreaterThan(1);
        LongUnaryOperator vToCommunity = result::community;
        var modularityCalculator = ModularityCalculator.create(
            TerminationFlag.RUNNING_TRUE,
            myGraph,
            vToCommunity,
            new Concurrency(4)
        );
        double calculatedModularity = modularityCalculator.compute().totalModularity();
        assertThat(result.modularity()).isCloseTo(calculatedModularity, Offset.offset(1e-5));
    }

    @Nested
    @GdlExtension
    class ProgressTrackingTest {

        @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 0)
        private static final String DB_CYPHER =
            """
                CREATE
                    (a:Node {seed: 1,seed2: -1}),
                    (b:Node {seed: 1,seed2: 10}),
                    (c:Node {seed: 1}),
                    (d:Node {seed: 1}),
                    (e:Node {seed: 1}),
                    (f:Node {seed: 1}),
                    (g:Node {seed: 2}),
                    (h:Node {seed: 2}),
                    (i:Node {seed: 2}),
                    (j:Node {seed: 42}),
                    (k:Node {seed: 42}),
                    (l:Node {seed: 42}),
                    (m:Node {seed: 42}),
                    (n:Node {seed: 42}),
                    (x:Node {seed: 1}),

                    (a)-[:TYPE_OUT {weight: 1.0}]->(b),
                    (a)-[:TYPE_OUT {weight: 1.0}]->(d),
                    (a)-[:TYPE_OUT {weight: 1.0}]->(f),
                    (b)-[:TYPE_OUT {weight: 1.0}]->(d),
                    (b)-[:TYPE_OUT {weight: 1.0}]->(x),
                    (b)-[:TYPE_OUT {weight: 1.0}]->(g),
                    (b)-[:TYPE_OUT {weight: 1.0}]->(e),
                    (c)-[:TYPE_OUT {weight: 1.0}]->(x),
                    (c)-[:TYPE_OUT {weight: 1.0}]->(f),
                    (d)-[:TYPE_OUT {weight: 1.0}]->(k),
                    (e)-[:TYPE_OUT {weight: 1.0}]->(x),
                    (e)-[:TYPE_OUT {weight: 0.01}]->(f),
                    (e)-[:TYPE_OUT {weight: 1.0}]->(h),
                    (f)-[:TYPE_OUT {weight: 1.0}]->(g),
                    (g)-[:TYPE_OUT {weight: 1.0}]->(h),
                    (h)-[:TYPE_OUT {weight: 1.0}]->(i),
                    (h)-[:TYPE_OUT {weight: 1.0}]->(j),
                    (i)-[:TYPE_OUT {weight: 1.0}]->(k),
                    (j)-[:TYPE_OUT {weight: 1.0}]->(k),
                    (j)-[:TYPE_OUT {weight: 1.0}]->(m),
                    (j)-[:TYPE_OUT {weight: 1.0}]->(n),
                    (k)-[:TYPE_OUT {weight: 1.0}]->(m),
                    (k)-[:TYPE_OUT {weight: 1.0}]->(l),
                    (l)-[:TYPE_OUT {weight: 1.0}]->(n),
                    (m)-[:TYPE_OUT {weight: 1.0}]->(n)
            """;

        @Inject
        private TestGraph graph;

        @Test
        void testLogging() {
            var concurrency = new Concurrency(4);

            var parameters =  LouvainParameters.createWithDefaults(concurrency,null);

            var progressTrackerWithLog = TestProgressTrackerHelper.create(
                CommunityAlgorithmTasks.louvain(
                    graph,
                    parameters
                ),
                concurrency
            );

            var louvain = new Louvain(
                graph,
                parameters,
                progressTrackerWithLog.progressTracker(),
                TerminationFlag.RUNNING_TRUE
            );
            louvain.compute();

            var log = progressTrackerWithLog.log();

            assertThat(log.containsMessage(INFO, ":: Start")).isTrue();
            assertThat(log.containsMessage(INFO, ":: ModularityOptimization")).isTrue();
            assertThat(log.containsMessage(INFO, ":: K1Coloring :: Start")).isTrue();
            assertThat(log.containsMessage(INFO, ":: Finished")).isTrue();
        }
    }

}
