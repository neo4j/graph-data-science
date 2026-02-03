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
package org.neo4j.gds.hits;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProgressTrackerHelper;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.indexinverse.InverseRelationshipsParameters;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
@Nested
class HitsWithInvertedIndexValidationTest {

    private static final String CYPHER_QUERY =
        "CREATE" +
            "  (a :Node)," +
            "  (b :Node)," +
            "  (c  :Node)," +
            "  (a)-[:R]->(b),"  +
            "  (b)-[:R]->(c)";

    @GdlExtension
    @Nested
    class WithoutInvertedIndex {

        @GdlGraph
        private static final String QUERY_1 = CYPHER_QUERY;

        @Inject
        GraphStore graphStore;

        @Inject
        IdFunction idFunction;

        @Test
        void shouldCreateInverseIndex() {
            var config = HitsConfigImpl.builder().relationshipTypes(List.of("R")).build();

            var invertedParameters = new InverseRelationshipsParameters(
                new Concurrency(1),
                Set.of(RelationshipType.of("R"))
            );
            var hitsWithInvertedIndex = new HitsWithInvertedIndexValidation(
                ProgressTracker.NULL_TRACKER,
                invertedParameters,
                graphStore,
                Set.of(NodeLabel.of("Node")),
                Set.of(RelationshipType.of("R")),
                (graph) -> new Hits(
                    graph,
                    config,
                    DefaultPool.INSTANCE,
                    ProgressTracker.NULL_TRACKER,
                    TerminationFlag.RUNNING_TRUE
                )
            );

            //result-check
            var result = hitsWithInvertedIndex.compute();

            var auth = result.pregelResult().nodeValues().doubleProperties("auth");
            var hub = result.pregelResult().nodeValues().doubleProperties("hub");

            double authValue = 0.7071067811865475;
            double hubValue = 0.7071067811865476;

            var a = graphStore.nodes().toMappedNodeId(idFunction.of("a"));
            var b = graphStore.nodes().toMappedNodeId(idFunction.of("b"));
            var c = graphStore.nodes().toMappedNodeId(idFunction.of("c"));
            var expectedResultMap = Map.of(
                a, new double[]{0.0, hubValue},
                b, new double[]{authValue, hubValue},
                c, new double[]{authValue, 0.0}
            );

            for (long i = 0; i < 3; ++i) {
                var expected = expectedResultMap.get(i);
                assertThat(auth.get(i)).isCloseTo(expected[0], Offset.offset(1e-6));
                assertThat(hub.get(i)).isCloseTo(expected[1], Offset.offset(1e-6));
            }

            //graphstore modification-check
            var graph = graphStore.getUnion();
            assertThat(graphStore.inverseIndexedRelationshipTypes()).contains(RelationshipType.of("R"));
            assertThat(graph.degreeInverse(a)).isEqualTo(0);
            assertThat(graph.degreeInverse(b)).isEqualTo(1);
            assertThat(graph.degreeInverse(c)).isEqualTo(1);

        }

        @Test
        void shouldLogProgress() {

            var config = HitsConfigImpl.builder().hitsIterations(1).relationshipTypes(List.of("R")).build();
            var invertedParameters = new InverseRelationshipsParameters(
                new Concurrency(1),
                Set.of(RelationshipType.of("R"))
            );

            var fullTask = HitsProgressTrackerCreator.progressTaskWithInvertedIndex(
                3,
                config.maxIterations(),
                invertedParameters
            );

            var progressTrackerWithLog = TestProgressTrackerHelper.create(
                fullTask,
                new Concurrency(1)
            );

            var progressTracker = progressTrackerWithLog.progressTracker();
            var log = progressTrackerWithLog.log();

            new HitsWithInvertedIndexValidation(
                progressTracker,
                invertedParameters,
                graphStore,
                Set.of(NodeLabel.of("Node")),
                Set.of(RelationshipType.of("R")),
                (graph) -> new Hits(
                    graph,
                    config,
                    DefaultPool.INSTANCE,
                    progressTracker,
                    TerminationFlag.RUNNING_TRUE
                )
            ).compute();

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "HITS :: Start",
                    "HITS :: IndexInverse :: Start",
                    "HITS :: IndexInverse :: Create inverse relationships of type 'R' :: Start",
                    "HITS :: IndexInverse :: Create inverse relationships of type 'R' 33%",
                    "HITS :: IndexInverse :: Create inverse relationships of type 'R' 66%",
                    "HITS :: IndexInverse :: Create inverse relationships of type 'R' 100%",
                    "HITS :: IndexInverse :: Create inverse relationships of type 'R' :: Finished",
                    "HITS :: IndexInverse :: Build Adjacency list :: Start",
                    "HITS :: IndexInverse :: Build Adjacency list 100%",
                    "HITS :: IndexInverse :: Build Adjacency list :: Finished",
                    "HITS :: IndexInverse :: Finished",
                    "HITS :: HITS :: Start",
                    "HITS :: HITS :: Compute iteration 1 of 4 :: Start",
                    "HITS :: HITS :: Compute iteration 1 of 4 100%",
                    "HITS :: HITS :: Compute iteration 1 of 4 :: Finished",
                    "HITS :: HITS :: Master compute iteration 1 of 4 :: Start",
                    "HITS :: HITS :: Master compute iteration 1 of 4 100%",
                    "HITS :: HITS :: Master compute iteration 1 of 4 :: Finished",
                    "HITS :: HITS :: Compute iteration 2 of 4 :: Start",
                    "HITS :: HITS :: Compute iteration 2 of 4 100%",
                    "HITS :: HITS :: Compute iteration 2 of 4 :: Finished",
                    "HITS :: HITS :: Master compute iteration 2 of 4 :: Start",
                    "HITS :: HITS :: Master compute iteration 2 of 4 100%",
                    "HITS :: HITS :: Master compute iteration 2 of 4 :: Finished",
                    "HITS :: HITS :: Compute iteration 3 of 4 :: Start",
                    "HITS :: HITS :: Compute iteration 3 of 4 100%",
                    "HITS :: HITS :: Compute iteration 3 of 4 :: Finished",
                    "HITS :: HITS :: Master compute iteration 3 of 4 :: Start",
                    "HITS :: HITS :: Master compute iteration 3 of 4 100%",
                    "HITS :: HITS :: Master compute iteration 3 of 4 :: Finished",
                    "HITS :: HITS :: Compute iteration 4 of 4 :: Start",
                    "HITS :: HITS :: Compute iteration 4 of 4 100%",
                    "HITS :: HITS :: Compute iteration 4 of 4 :: Finished",
                    "HITS :: HITS :: Master compute iteration 4 of 4 :: Start",
                    "HITS :: HITS :: Master compute iteration 4 of 4 100%",
                    "HITS :: HITS :: Master compute iteration 4 of 4 :: Finished",
                    "HITS :: HITS :: Finished",
                    "HITS :: Finished"
                );
        }

        @GdlExtension
        @Nested
        class WithInvertedIndex {

            @GdlGraph(indexInverse = true)
            private static final String QUERY_1 = CYPHER_QUERY;

            @Inject
            GraphStore graphStore;

            @Inject
            IdFunction idFunction;

            @Test
            void shouldCreateInverseIndex() {
                var config = HitsConfigImpl.builder().relationshipTypes(List.of("R")).build();

                var invertedParameters = new InverseRelationshipsParameters(
                    new Concurrency(1),
                    Set.of()
                );
                var hitsWithInvertedIndex = new HitsWithInvertedIndexValidation(
                    ProgressTracker.NULL_TRACKER,
                    invertedParameters,
                    graphStore,
                    Set.of(NodeLabel.of("Node")),
                    Set.of(RelationshipType.of("R")),
                    (graph) -> new Hits(
                        graph,
                        config,
                        DefaultPool.INSTANCE,
                        ProgressTracker.NULL_TRACKER,
                        TerminationFlag.RUNNING_TRUE
                    )
                );


                //result-check
                var result = hitsWithInvertedIndex.compute();
                var auth = result.pregelResult().nodeValues().doubleProperties("auth");
                var hub = result.pregelResult().nodeValues().doubleProperties("hub");

                double authValue = 0.7071067811865475;
                double hubValue = 0.7071067811865476;

                var a = graphStore.nodes().toMappedNodeId(idFunction.of("a"));
                var b = graphStore.nodes().toMappedNodeId(idFunction.of("b"));
                var c = graphStore.nodes().toMappedNodeId(idFunction.of("c"));
                var expectedResultMap = Map.of(
                    a, new double[]{0.0, hubValue},
                    b, new double[]{authValue, hubValue},
                    c, new double[]{authValue, 0.0}
                );

                for (long i = 0; i < 3; ++i) {
                    var expected = expectedResultMap.get(i);
                    assertThat(auth.get(i)).isCloseTo(expected[0], Offset.offset(1e-6));
                    assertThat(hub.get(i)).isCloseTo(expected[1], Offset.offset(1e-6));
                }

                //graphstore modification-check
                var graph = graphStore.getUnion();
                assertThat(graphStore.inverseIndexedRelationshipTypes()).contains(RelationshipType.of("R"));
                assertThat(graph.degreeInverse(a)).isEqualTo(0);
                assertThat(graph.degreeInverse(b)).isEqualTo(1);
                assertThat(graph.degreeInverse(c)).isEqualTo(1);

            }

            @Test
            void shouldLogProgress() {

                var config = HitsConfigImpl.builder().hitsIterations(1).relationshipTypes(List.of("R")).build();
                var invertedParameters = new InverseRelationshipsParameters(
                    new Concurrency(1),
                    Set.of()
                );

                var fullTask = HitsProgressTrackerCreator.progressTaskWithInvertedIndex(
                    3,
                    config.maxIterations(),
                    invertedParameters
                );

                var progressTrackerWithLog = TestProgressTrackerHelper.create(
                    fullTask,
                    new Concurrency(1)
                );

                var progressTracker = progressTrackerWithLog.progressTracker();
                var log = progressTrackerWithLog.log();

                new HitsWithInvertedIndexValidation(
                    progressTracker,
                    invertedParameters,
                    graphStore,
                    Set.of(NodeLabel.of("Node")),
                    Set.of(RelationshipType.of("R")),
                    (graph) -> new Hits(
                        graph,
                        config,
                        DefaultPool.INSTANCE,
                        progressTracker,
                        TerminationFlag.RUNNING_TRUE
                    )
                ).compute();

                Assertions.assertThat(log.getMessages(TestLog.INFO))
                    .extracting(removingThreadId())
                    .extracting(replaceTimings())
                    .containsExactly(
                        "HITS :: Start",
                        "HITS :: IndexInverse :: Start",
                        "HITS :: IndexInverse :: Finished",
                        "HITS :: HITS :: Start",
                        "HITS :: HITS :: Compute iteration 1 of 4 :: Start",
                        "HITS :: HITS :: Compute iteration 1 of 4 100%",
                        "HITS :: HITS :: Compute iteration 1 of 4 :: Finished",
                        "HITS :: HITS :: Master compute iteration 1 of 4 :: Start",
                        "HITS :: HITS :: Master compute iteration 1 of 4 100%",
                        "HITS :: HITS :: Master compute iteration 1 of 4 :: Finished",
                        "HITS :: HITS :: Compute iteration 2 of 4 :: Start",
                        "HITS :: HITS :: Compute iteration 2 of 4 100%",
                        "HITS :: HITS :: Compute iteration 2 of 4 :: Finished",
                        "HITS :: HITS :: Master compute iteration 2 of 4 :: Start",
                        "HITS :: HITS :: Master compute iteration 2 of 4 100%",
                        "HITS :: HITS :: Master compute iteration 2 of 4 :: Finished",
                        "HITS :: HITS :: Compute iteration 3 of 4 :: Start",
                        "HITS :: HITS :: Compute iteration 3 of 4 100%",
                        "HITS :: HITS :: Compute iteration 3 of 4 :: Finished",
                        "HITS :: HITS :: Master compute iteration 3 of 4 :: Start",
                        "HITS :: HITS :: Master compute iteration 3 of 4 100%",
                        "HITS :: HITS :: Master compute iteration 3 of 4 :: Finished",
                        "HITS :: HITS :: Compute iteration 4 of 4 :: Start",
                        "HITS :: HITS :: Compute iteration 4 of 4 100%",
                        "HITS :: HITS :: Compute iteration 4 of 4 :: Finished",
                        "HITS :: HITS :: Master compute iteration 4 of 4 :: Start",
                        "HITS :: HITS :: Master compute iteration 4 of 4 100%",
                        "HITS :: HITS :: Master compute iteration 4 of 4 :: Finished",
                        "HITS :: HITS :: Finished",
                        "HITS :: Finished"
                    );
            }

        }
    }

}
