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
package org.neo4j.gds.similarity.filteredknn;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.FilteringParameters;
import org.neo4j.gds.similarity.NodeFilterSpec;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filtering.NodeIdNodeFilterSpec;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.KnnNodePropertySpec;
import org.neo4j.gds.similarity.knn.KnnParametersSansNodeCount;
import org.neo4j.gds.similarity.knn.KnnSampler;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class FilteredKnnTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a { knn: 1.2, prop: 1.0 } )" +
        ", (b { knn: 1.1, prop: 5.0 } )" +
        ", (c { knn: 42.0, prop: 10.0 } )";
    @Inject
    private TestGraph graph;

    @GdlGraph(graphNamePrefix = "simThreshold")
    private static final String nodeCreateQuery =
        "CREATE " +
        "  (alice:Person {age: 23})" +
        " ,(carol:Person {age: 24})" +
        " ,(eve:Person {age: 34})" +
        " ,(bob:Person {age: 30})";
    @Inject
    private TestGraph simThresholdGraph;

    @GdlGraph(graphNamePrefix = "multPropMissing")
    private static final String nodeCreateMultipleQuery =
        "CREATE " +
        "  (a: P1 {prop1: 1.0, prop2: 5.0})" +
        " ,(b: P1 {prop2 : 5.0})" +
        " ,(c: P1 {prop2 : 10.0})" +
        " ,(d: P1 {prop3 : 10.0})";
    @Inject
    private TestGraph multPropMissingGraph;

    @Test
    void shouldRunJustLikeKnnWhenYouDoNotSpecifySourceNodeFilterOrTargetNodeFilter() {
        IdFunction idFunction = graph::toMappedNodeId;

        var knnSans = KnnParametersSansNodeCount.create(
            new Concurrency(1),
            100,
            0,
            0.001,
            0.5,
            1,
            0,
            10,
            1_000,
            KnnSampler.SamplerType.UNIFORM,
            Optional.of(19L),
            List.of(new KnnNodePropertySpec("knn"))
        );

        var filteredSans = new FilteredKnnParametersSansNodeCount(
            knnSans,
            new FilteringParameters(NodeFilterSpec.noOp,NodeFilterSpec.noOp),
            false
        );
        var params = filteredSans.finalize(graph.nodeCount());


        var knnContext = KnnContext.empty();

        var knn = FilteredKnn.createWithoutSeeding(graph, params, knnContext, TerminationFlag.RUNNING_TRUE);
        var result = knn.compute();

        assertThat(result).isNotNull();
        assertThat(result.similarityResultStream().count()).isEqualTo(3);

        long nodeAId = idFunction.of("a");
        long nodeBId = idFunction.of("b");
        long nodeCId = idFunction.of("c");

        assertCorrectNeighborList(result, nodeAId, nodeBId);
        assertCorrectNeighborList(result, nodeBId, nodeAId);
        assertCorrectNeighborList(result, nodeCId, nodeAId);
    }

    private void assertCorrectNeighborList(FilteredKnnResult result, long nodeId, long... expectedNeighbors) {
        List<SimilarityResult> similarityResults = result.similarityResultStream()
            .filter(sr -> sr.sourceNodeId() == nodeId)
            .collect(Collectors.toList());

        assertThat(similarityResults)
            .isSortedAccordingTo(Comparator.comparingDouble((s) -> -s.similarity));

        var justNeighbours = similarityResults.stream().mapToLong(SimilarityResult::targetNodeId).toArray();
        assertThat(justNeighbours)
            .doesNotContain(nodeId)
            .containsAnyOf(expectedNeighbors)
            .doesNotHaveDuplicates()
            .hasSizeLessThanOrEqualTo(expectedNeighbors.length);
    }

    @Nested
    class SourceNodeFilterTest {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a { knn: 1.2 } )" +
            ", (b { knn: 1.1 } )" +
            ", (c { knn: 2.1 } )" +
            ", (d { knn: 3.1 } )" +
            ", (e { knn: 4.1 } )";

        @Test
        void shouldOnlyProduceResultsForFilteredSourceNode() {
            var filteredSourceNode = "a";
            var originalId = graph.toOriginalNodeId(filteredSourceNode);

            var knnSans = KnnParametersSansNodeCount.create(
                new Concurrency(1),
                1,
                0,
                0.001,
                0.5,
                3,
                0,
                0,
                1_000,
                KnnSampler.SamplerType.UNIFORM,
                Optional.of(20L),
                List.of(new KnnNodePropertySpec("knn"))
            );

            var filteredSans = new FilteredKnnParametersSansNodeCount(
                knnSans,
                new FilteringParameters(new NodeIdNodeFilterSpec(Set.of(originalId)),NodeFilterSpec.noOp),
                false
            );
            var params = filteredSans.finalize(graph.nodeCount());

            var knnContext = KnnContext.empty();
            var knn = FilteredKnn.createWithoutSeeding(graph, params, knnContext, TerminationFlag.RUNNING_TRUE);

            var result = knn.compute();

            assertThat(result.similarityResultStream()
                .map(sr -> sr.node1)
                .collect(Collectors.toSet())
            ).containsExactly(graph.toMappedNodeId(filteredSourceNode));
        }

        @Test
        void shouldOnlyProduceResultsForMultipleFilteredSourceNode() {
            var filteredNodes = List.of("a", "b");
            var originalFilteredNodes = filteredNodes.stream().map(graph::toOriginalNodeId).toList();

            var knnSans = KnnParametersSansNodeCount.create(
                new Concurrency(1),
                1,
                0,
                0.001,
                0.5,
                3,
                0,
                0,
                1_000,
                KnnSampler.SamplerType.UNIFORM,
                Optional.of(20L),
                List.of(new KnnNodePropertySpec("knn"))
            );

            var filteredSans = new FilteredKnnParametersSansNodeCount(
                knnSans,
                new FilteringParameters(new NodeIdNodeFilterSpec(new HashSet<>(originalFilteredNodes)),NodeFilterSpec.noOp),
                false
            );

            var params = filteredSans.finalize(graph.nodeCount());

            var knnContext = KnnContext.empty();
            var knn = FilteredKnn.createWithoutSeeding(graph, params, knnContext, TerminationFlag.RUNNING_TRUE);
            var result = knn.compute();

            assertThat(result.similarityResultStream()
                .map(sr -> sr.node1)
                .collect(Collectors.toSet())
            ).isEqualTo(filteredNodes.stream().map(graph::toMappedNodeId).collect(Collectors.toSet()));
        }
    }

    @Nested
    class TargetNodeFiltering {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a { knn: 1.2 } )" +
            ", (b { knn: 1.1 } )" +
            ", (c { knn: 2.1 } )" +
            ", (d { knn: 3.1 } )" +
            ", (e { knn: 4.1 } )";

        @Test
        void shouldOnlyProduceResultsForFilteredTargetNode() {
            var targetNode = "a";
            var originalNode = graph.toOriginalNodeId(targetNode);

            var knnSans = KnnParametersSansNodeCount.create(
                new Concurrency(1),
                1,
                0,
                0.001,
                0.5,
                3,
                0,
                0,
                1_000,
                KnnSampler.SamplerType.UNIFORM,
                Optional.of(20L),
                List.of(new KnnNodePropertySpec("knn"))
            );

            var filteredSans = new FilteredKnnParametersSansNodeCount(
                knnSans,
                new FilteringParameters(NodeFilterSpec.noOp,new NodeIdNodeFilterSpec(Set.of(originalNode))),
                false
            );
            var params = filteredSans.finalize(graph.nodeCount());


            var knnContext = KnnContext.empty();


            var knn = FilteredKnn.createWithoutSeeding(graph, params, knnContext, TerminationFlag.RUNNING_TRUE);
            var result = knn.compute();

            assertThat(result.similarityResultStream()
                .map(SimilarityResult::targetNodeId)
                .collect(Collectors.toSet())
            ).containsExactly(graph.toMappedNodeId(targetNode));
        }

        @Test
        void shouldOnlyProduceResultsForFilteredTargetNodes() {
            var targetNodes = List.of("a", "b");
            var originalTargetNodes = targetNodes.stream().map(graph::toOriginalNodeId).toList();

            var knnSans = KnnParametersSansNodeCount.create(
                new Concurrency(1),
                1,
                0,
                0.001,
                0.5,
                3,
                0,
                0,
                1_000,
                KnnSampler.SamplerType.UNIFORM,
                Optional.of(20L),
                List.of(new KnnNodePropertySpec("knn"))
            );

            var filteredSans = new FilteredKnnParametersSansNodeCount(
                knnSans,
                new FilteringParameters(NodeFilterSpec.noOp,new NodeIdNodeFilterSpec(new HashSet<>(originalTargetNodes))),
                false
            );
            var params = filteredSans.finalize(graph.nodeCount());

            var knnContext = KnnContext.empty();

            var knn = FilteredKnn.createWithoutSeeding(graph, params, knnContext, TerminationFlag.RUNNING_TRUE);
            var result = knn.compute();

            assertThat(result.similarityResultStream()
                .map(SimilarityResult::targetNodeId)
                .collect(Collectors.toSet())
            ).isEqualTo(targetNodes.stream().map(graph::toMappedNodeId).collect(Collectors.toSet()));
        }
    }

    @Nested
    class TargetNodeFilteringAndDuplicates {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a { knn: 1.2 } )" +
            ", (b { knn: 1.1 } )" +
            ", (c { knn: 2.1 } )" +
            ", (d { knn: 3.1 } )" +
            ", (e { knn: 4.1 } )";

        @Test
        void shouldIgnoreDuplicates() {

            var knnSans = KnnParametersSansNodeCount.create(
                new Concurrency(1),
                100,
                0,
                0.001,
                0.5,
                42,
                0,
                10,
                1_000,
                KnnSampler.SamplerType.UNIFORM,
                Optional.of(19L),
                List.of(new KnnNodePropertySpec("knn"))
            );

            var filteredSans = new FilteredKnnParametersSansNodeCount(
                knnSans,
                new FilteringParameters(NodeFilterSpec.noOp,NodeFilterSpec.noOp),
                false
            );
            var params = filteredSans.finalize(graph.nodeCount());


            var knnContext = KnnContext.empty();

            var knn = FilteredKnn.createWithoutSeeding(graph, params, knnContext, TerminationFlag.RUNNING_TRUE);
            var result = knn.compute();

            /*
             * Ok we want to express that, for each source node, the target nodes found have no duplicates.
             * First, group the results
             */
            Map<Long, List<SimilarityResult>> resultsPerSourceNode = result
                .similarityResultStream()
                .collect(Collectors.groupingBy(SimilarityResult::sourceNodeId));

            // now for each result, see that there are no duplicates
            resultsPerSourceNode
                .values()
                .forEach(similarityResultList -> assertThat(similarityResultList
                    .stream()
                    .mapToLong(SimilarityResult::targetNodeId)).doesNotHaveDuplicates());
        }
    }

    @Nested
    class TargetNodeFilteringAndSeeding {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (x { knn: 100.2 } )" +
            "  (y { knn: 1000.2 } )" +
            "  (z { knn: 10000.2 } )" +
            "  (a { knn: 1.2 } )" +
            ", (b { knn: 1.1 } )" +
            ", (c { knn: 2.1 } )" +
            ", (d { knn: 3.1 } )" +
            ", (e { knn: 4.1 } )";

        /**
         * Testing seeding at this level is difficult, you rely on a leap and a prayer. Here is a stab at it. If this
         * becomes unmaintainable, rely on just {@link org.neo4j.gds.similarity.filteredknn.TargetNodeFilteringTest}
         * instead.
         */
        @Test
        void shouldSeedResultSet() {
            /*
             * This is a bit convoluted: seeding will take the first k nodes regardless of score. Consider node a, it
             * will score very poorly with nodes x, y and z, but x, y and z will be its seeds nonetheless.
             *
             * So here we confirm the first three nodes are the undesirables ones, and tacit knowledge says they will
             * form the seed.
             */
            var targetNodeX = graph.toMappedNodeId("x");
            var targetNodeY = graph.toMappedNodeId("y");
            var targetNodeZ = graph.toMappedNodeId("z");
            var targetNodeA = graph.toMappedNodeId("a");
            assertThat(targetNodeX).isLessThan(targetNodeY).isLessThan(targetNodeZ).isLessThan(targetNodeA);

            var knnSans = KnnParametersSansNodeCount.create(
                new Concurrency(1),
                100,
                0,
                0.001,
                0.5,
                4,
                0,
                10,
                1_000,
                KnnSampler.SamplerType.UNIFORM,
                Optional.of(87L),
                List.of(new KnnNodePropertySpec("knn"))
            );
            //no target node filter specified -> everything is a target node
            var filteredSans = new FilteredKnnParametersSansNodeCount(
                knnSans,
                new FilteringParameters(NodeFilterSpec.noOp,NodeFilterSpec.noOp),
                true
            );
            var params = filteredSans.finalize(graph.nodeCount());

            var knnContext = KnnContext.empty();

            var knn = FilteredKnn.createWithDefaultSeeding(graph, params, knnContext, TerminationFlag.RUNNING_TRUE);
            var result = knn.compute();

            /*
             * Now let's look at a and it's highest scoring neighbours. They should _not_ be x, y or z because we know
             * those will score very poorly, even if those were the seed nodes.
             */
            Stream<Long> highestScoringNeighboursOfNodeA = result.similarityResultStream()
                .filter(sr -> sr.sourceNodeId() == targetNodeA)
                .map(SimilarityResult::targetNodeId);
            assertThat(highestScoringNeighboursOfNodeA).doesNotContain(targetNodeX, targetNodeY, targetNodeZ);
        }
    }
}
