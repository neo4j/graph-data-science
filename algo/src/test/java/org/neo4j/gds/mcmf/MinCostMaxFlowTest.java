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
package org.neo4j.gds.mcmf;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.InputNodes;
import org.neo4j.gds.ListInputNodes;
import org.neo4j.gds.MapInputNodes;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.maxflow.MaxFlowParameters;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.gdlGraphStore;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

class MinCostMaxFlowTest {

    void testGraph(
        GraphStore graphStore,
        InputNodes sourceNodes,
        InputNodes targetNodes,
        double expectedFlow,
        double expectedCost
    ) {

        var maxFlowParameters = new MaxFlowParameters(
            sourceNodes,
            targetNodes,
            new Concurrency(1),
            0.5,
            true
        );
        var params = new MCMFParameters(
            maxFlowParameters,
            6,
            List.of(NodeLabel.of("n")),
            List.of(RelationshipType.of("R")),
            "u",
            "c"
        );
        var x =  MinCostMaxFlow.create(
            graphStore,
           params,
            ProgressTracker.NULL_TRACKER
        );

        var result = x.compute();
        double TOLERANCE = 1E-10;
        assertThat(result.totalFlow()).isCloseTo(expectedFlow, Offset.offset(TOLERANCE));
        assertThat(result.totalCost()).isCloseTo(expectedCost, Offset.offset(TOLERANCE));
    }

    @Test
    void testGraphWithThreeNodes() {
        var graphStore = gdlGraphStore(
            """
                CREATE
                    (a0:n),
                    (a1:n),
                    (a2:n),
                    (a0)-[:R {u:1, c:10}]->(a1),
                    (a0)-[:R {u:1, c:100}]->(a1),
                    (a1)-[:R {u:1, c:1}]->(a2)
                """,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
        var idMap = graphStore.nodes();

        var sourceNodes = new ListInputNodes(List.of(idMap.toMappedNodeId(0)));
        var targetNodes = new ListInputNodes(List.of(idMap.toMappedNodeId(2)));

        testGraph(graphStore, sourceNodes, targetNodes, 1.0, 11.0);
    }

    @Test
    void testGraphWithFourNodes() {
        var graphStore = gdlGraphStore(
            """
                CREATE
                    (a0:n),
                    (a1:n),
                    (a2:n),
                    (a3:n),
                    (a0)-[:R {u: 20, c:5}]->(a1),
                    (a0)-[:R {u: 15, c:10}]->(a2),
                    (a1)-[:R {u: 10, c:8}]->(a3),
                    (a2)-[:R {u: 12, c:6}]->(a3)
                """,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
        var idMap = graphStore.nodes();

        var sourceNodes = new ListInputNodes(List.of(idMap.toMappedNodeId(0)));
        var targetNodes = new ListInputNodes(List.of(idMap.toMappedNodeId(3)));

        testGraph(graphStore, sourceNodes, targetNodes, 22.0, 322.0);
    }

    @Test
    void testGraphWithFiveNodes() {
        var graphStore = gdlGraphStore(
            """
                CREATE
                    (a0:n),
                    (a1:n),
                    (a2:n),
                    (a3:n),
                    (a4:n),
                    (a0)-[:R {u: 30, c:10}]->(a1),
                    (a0)-[:R {u: 25, c:15}]->(a2),
                    (a1)-[:R {u: 20, c:20}]->(a3),
                    (a1)-[:R {u: 15, c:12}]->(a4),
                    (a2)-[:R {u: 18, c:8}]->(a3),
                    (a2)-[:R {u: 22, c:18}]->(a1),
                    (a3)-[:R {u: 25, c:5}]->(a4),
                    (a3)-[:R {u: 10, c:25}]->(a0),
                    (a4)-[:R {u: 12, c:30}]->(a2),
                    (a4)-[:R {u: 20, c:22}]->(a0)
                """,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
        var idMap = graphStore.nodes();

        var sourceNodes = new ListInputNodes(List.of(idMap.toMappedNodeId(0)));
        var targetNodes = new ListInputNodes(List.of(idMap.toMappedNodeId(4)));

        testGraph(graphStore, sourceNodes, targetNodes, 40.0, 1079.0);
    }
    @Nested
    @GdlExtension
    class GraphWithTenNodes{
        @GdlGraph
        private static final String GRAPH =
            """
              CREATE
                  (a0:n),
                  (a1:n),
                  (a2:n),
                  (a3:n),
                  (a4:n),
                  (a5:n),
                  (a6:n),
                  (a7:n),
                  (a8:n),
                  (a9:n),
                  (a0)-[:R {u: 50, c:98}]->(a8),
                  (a0)-[:R {u: 54, c:6}]->(a1),
                  (a0)-[:R {u: 34, c:66}]->(a7),
                  (a0)-[:R {u: 63, c:52}]->(a3),
                  (a0)-[:R {u: 39, c:62}]->(a6),
                  (a1)-[:R {u: 46, c:75}]->(a9),
                  (a1)-[:R {u: 28, c:65}]->(a0),
                  (a1)-[:R {u: 18, c:37}]->(a2),
                  (a1)-[:R {u: 18, c:97}]->(a5),
                  (a1)-[:R {u: 13, c:80}]->(a7),
                  (a2)-[:R {u: 33, c:69}]->(a5),
                  (a2)-[:R {u: 91, c:78}]->(a8),
                  (a2)-[:R {u: 19, c:40}]->(a4),
                  (a2)-[:R {u: 13, c:94}]->(a1),
                  (a2)-[:R {u: 10, c:88}]->(a6),
                  (a3)-[:R {u: 43, c:61}]->(a4),
                  (a3)-[:R {u: 72, c:13}]->(a7),
                  (a3)-[:R {u: 46, c:56}]->(a6),
                  (a3)-[:R {u: 41, c:79}]->(a5),
                  (a3)-[:R {u: 82, c:27}]->(a0),
                  (a4)-[:R {u: 71, c:62}]->(a3),
                  (a4)-[:R {u: 57, c:67}]->(a9),
                  (a4)-[:R {u: 34, c:8}]->(a6),
                  (a4)-[:R {u: 71, c:2}]->(a2),
                  (a4)-[:R {u: 12, c:93}]->(a7),
                  (a5)-[:R {u: 52, c:91}]->(a2),
                  (a5)-[:R {u: 86, c:81}]->(a9),
                  (a5)-[:R {u: 1, c:79}]->(a1),
                  (a5)-[:R {u: 64, c:43}]->(a3),
                  (a5)-[:R {u: 32, c:94}]->(a8),
                  (a6)-[:R {u: 42, c:91}]->(a4),
                  (a6)-[:R {u: 9, c:25}]->(a3),
                  (a6)-[:R {u: 73, c:29}]->(a0),
                  (a6)-[:R {u: 31, c:19}]->(a2),
                  (a6)-[:R {u: 70, c:58}]->(a9),
                  (a7)-[:R {u: 12, c:11}]->(a3),
                  (a7)-[:R {u: 41, c:66}]->(a0),
                  (a7)-[:R {u: 63, c:14}]->(a4),
                  (a7)-[:R {u: 39, c:71}]->(a1),
                  (a7)-[:R {u: 38, c:91}]->(a8),
                  (a8)-[:R {u: 16, c:71}]->(a9),
                  (a8)-[:R {u: 43, c:70}]->(a0),
                  (a8)-[:R {u: 27, c:78}]->(a2),
                  (a8)-[:R {u: 71, c:76}]->(a5),
                  (a8)-[:R {u: 37, c:57}]->(a7),
                  (a9)-[:R {u: 12, c:77}]->(a4),
                  (a9)-[:R {u: 50, c:41}]->(a8),
                  (a9)-[:R {u: 74, c:31}]->(a1),
                  (a9)-[:R {u: 38, c:24}]->(a5),
                  (a9)-[:R {u: 25, c:24}]->(a6)
              """;

        @Inject
        private GraphStore graphStore;

        @Inject
        private IdFunction idFunction;

    @Test
    void testGraphWithTenNodes() {


        InputNodes sourceNodes = new ListInputNodes(List.of(idFunction.of("a1")));
        InputNodes targetNodes = new ListInputNodes(List.of(idFunction.of("a5")));

        testGraph(graphStore, sourceNodes, targetNodes, 123.0, 16_575.0);

    }

    @Test
    void shouldLogProgress() {

            var log = new GdsTestLog();
            var testTracker = TestProgressTracker.create(
                MinCostMaxFlowTask.create(),
                new LoggerForProgressTrackingAdapter(log),
                new Concurrency(4),
                EmptyTaskRegistryFactory.INSTANCE
            );

        InputNodes sourceNodes = new ListInputNodes(List.of(idFunction.of("a1")));
        InputNodes targetNodes = new ListInputNodes(List.of(idFunction.of("a5")));

        var maxFlowParameters = new MaxFlowParameters(
            sourceNodes,
            targetNodes,
            new Concurrency(1),
            0.5,
            true
        );
        var params = new MCMFParameters(
            maxFlowParameters,
            6,
            List.of(NodeLabel.of("n")),
            List.of(RelationshipType.of("R")),
            "u",
            "c"
        );
        MinCostMaxFlow.create(
                graphStore,
                params,
                testTracker
            ).compute();

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                "MinCostMaxFlow :: Start",
                "MinCostMaxFlow :: MaxFlow :: Start",
                "MinCostMaxFlow :: MaxFlow 15%",
                "MinCostMaxFlow :: MaxFlow 61%",
                "MinCostMaxFlow :: MaxFlow 100%",
                "MinCostMaxFlow :: MaxFlow :: Finished",
                "MinCostMaxFlow :: Cost refinement :: Start",
                "MinCostMaxFlow :: Cost refinement :: Refine 1 :: Start",
                "MinCostMaxFlow :: Cost refinement :: Refine 1 100%",
                "MinCostMaxFlow :: Cost refinement :: Refine 1 :: Finished",
                "MinCostMaxFlow :: Cost refinement :: Refine 2 :: Start",
                "MinCostMaxFlow :: Cost refinement :: Refine 2 100%",
                "MinCostMaxFlow :: Cost refinement :: Refine 2 :: Finished",
                "MinCostMaxFlow :: Cost refinement :: Refine 3 :: Start",
                "MinCostMaxFlow :: Cost refinement :: Refine 3 100%",
                "MinCostMaxFlow :: Cost refinement :: Refine 3 :: Finished",
                "MinCostMaxFlow :: Cost refinement :: Refine 4 :: Start",
                "MinCostMaxFlow :: Cost refinement :: Refine 4 100%",
                "MinCostMaxFlow :: Cost refinement :: Refine 4 :: Finished",
                "MinCostMaxFlow :: Cost refinement :: Finished",
                "MinCostMaxFlow :: Finished"
                );
        }




}
    @Test
    void testGraphWithTwentyNodes() {
        var graphStore = gdlGraphStore("""
            CREATE
              (a0:n), (a1:n), (a2:n), (a3:n), (a4:n), (a5:n), (a6:n), (a7:n), (a8:n), (a9:n),
              (a10:n), (a11:n), (a12:n), (a13:n), (a14:n), (a15:n), (a16:n), (a17:n), (a18:n), (a19:n),
              (a0)-[:R {u: 50, c:98}]->(a5),
              (a0)-[:R {u: 54, c:6}]->(a4),
              (a0)-[:R {u: 34, c:66}]->(a19),
              (a0)-[:R {u: 63, c:52}]->(a10),
              (a0)-[:R {u: 39, c:62}]->(a18),
              (a1)-[:R {u: 46, c:75}]->(a2),
              (a1)-[:R {u: 28, c:65}]->(a5),
              (a1)-[:R {u: 18, c:37}]->(a17),
              (a1)-[:R {u: 18, c:97}]->(a10),
              (a1)-[:R {u: 13, c:80}]->(a19),
              (a2)-[:R {u: 33, c:69}]->(a14),
              (a2)-[:R {u: 91, c:78}]->(a1),
              (a2)-[:R {u: 19, c:40}]->(a7),
              (a2)-[:R {u: 13, c:94}]->(a10),
              (a2)-[:R {u: 10, c:88}]->(a6),
              (a3)-[:R {u: 43, c:61}]->(a4),
              (a3)-[:R {u: 72, c:13}]->(a19),
              (a3)-[:R {u: 46, c:56}]->(a6),
              (a3)-[:R {u: 41, c:79}]->(a11),
              (a3)-[:R {u: 82, c:27}]->(a14),
              (a4)-[:R {u: 71, c:62}]->(a3),
              (a4)-[:R {u: 57, c:67}]->(a12),
              (a4)-[:R {u: 34, c:8}]->(a0),
              (a4)-[:R {u: 71, c:2}]->(a7),
              (a4)-[:R {u: 12, c:93}]->(a16),
              (a5)-[:R {u: 52, c:91}]->(a7),
              (a5)-[:R {u: 86, c:81}]->(a0),
              (a5)-[:R {u: 1, c:79}]->(a9),
              (a5)-[:R {u: 64, c:43}]->(a18),
              (a5)-[:R {u: 32, c:94}]->(a1),
              (a6)-[:R {u: 42, c:91}]->(a12),
              (a6)-[:R {u: 9, c:25}]->(a3),
              (a6)-[:R {u: 73, c:29}]->(a13),
              (a6)-[:R {u: 31, c:19}]->(a16),
              (a6)-[:R {u: 70, c:58}]->(a2),
              (a7)-[:R {u: 12, c:11}]->(a5),
              (a7)-[:R {u: 41, c:66}]->(a2),
              (a7)-[:R {u: 63, c:14}]->(a15),
              (a7)-[:R {u: 39, c:71}]->(a4),
              (a7)-[:R {u: 38, c:91}]->(a8),
              (a8)-[:R {u: 16, c:71}]->(a15),
              (a8)-[:R {u: 43, c:70}]->(a11),
              (a8)-[:R {u: 27, c:78}]->(a17),
              (a8)-[:R {u: 71, c:76}]->(a13),
              (a8)-[:R {u: 37, c:57}]->(a7),
              (a9)-[:R {u: 12, c:77}]->(a13),
              (a9)-[:R {u: 50, c:41}]->(a17),
              (a9)-[:R {u: 74, c:31}]->(a5),
              (a9)-[:R {u: 38, c:24}]->(a19),
              (a9)-[:R {u: 25, c:24}]->(a12),
              (a10)-[:R {u: 5, c:79}]->(a12),
              (a10)-[:R {u: 85, c:34}]->(a18),
              (a10)-[:R {u: 61, c:9}]->(a2),
              (a10)-[:R {u: 12, c:87}]->(a0),
              (a10)-[:R {u: 97, c:17}]->(a1),
              (a11)-[:R {u: 20, c:5}]->(a8),
              (a11)-[:R {u: 11, c:90}]->(a16),
              (a11)-[:R {u: 70, c:88}]->(a3),
              (a11)-[:R {u: 51, c:91}]->(a18),
              (a11)-[:R {u: 68, c:36}]->(a15),
              (a12)-[:R {u: 67, c:31}]->(a6),
              (a12)-[:R {u: 28, c:87}]->(a4),
              (a12)-[:R {u: 76, c:54}]->(a10),
              (a12)-[:R {u: 75, c:36}]->(a14),
              (a12)-[:R {u: 58, c:64}]->(a9),
              (a13)-[:R {u: 85, c:83}]->(a9),
              (a13)-[:R {u: 90, c:46}]->(a14),
              (a13)-[:R {u: 11, c:42}]->(a16),
              (a13)-[:R {u: 79, c:15}]->(a6),
              (a13)-[:R {u: 63, c:76}]->(a8),
              (a14)-[:R {u: 81, c:43}]->(a2),
              (a14)-[:R {u: 25, c:32}]->(a13),
              (a14)-[:R {u: 3, c:94}]->(a15),
              (a14)-[:R {u: 35, c:15}]->(a12),
              (a14)-[:R {u: 91, c:29}]->(a3),
              (a15)-[:R {u: 48, c:22}]->(a8),
              (a15)-[:R {u: 43, c:55}]->(a17),
              (a15)-[:R {u: 8, c:13}]->(a14),
              (a15)-[:R {u: 19, c:90}]->(a7),
              (a15)-[:R {u: 29, c:6}]->(a11),
              (a16)-[:R {u: 74, c:82}]->(a19),
              (a16)-[:R {u: 69, c:78}]->(a11),
              (a16)-[:R {u: 88, c:10}]->(a13),
              (a16)-[:R {u: 4, c:16}]->(a6),
              (a16)-[:R {u: 82, c:25}]->(a4),
              (a17)-[:R {u: 78, c:74}]->(a9),
              (a17)-[:R {u: 16, c:51}]->(a18),
              (a17)-[:R {u: 12, c:48}]->(a15),
              (a17)-[:R {u: 15, c:5}]->(a8),
              (a17)-[:R {u: 78, c:3}]->(a1),
              (a18)-[:R {u: 25, c:24}]->(a10),
              (a18)-[:R {u: 92, c:16}]->(a17),
              (a18)-[:R {u: 62, c:27}]->(a5),
              (a18)-[:R {u: 94, c:8}]->(a11),
              (a18)-[:R {u: 87, c:3}]->(a0),
              (a19)-[:R {u: 70, c:55}]->(a3),
              (a19)-[:R {u: 80, c:13}]->(a16),
              (a19)-[:R {u: 34, c:9}]->(a9),
              (a19)-[:R {u: 29, c:10}]->(a0),
              (a19)-[:R {u: 83, c:39}]->(a1)
            """,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
        var idMap = graphStore.nodes();

        InputNodes sourceNodes = new ListInputNodes(List.of(idMap.toMappedNodeId(8)));
        InputNodes targetNodes = new ListInputNodes(List.of(idMap.toMappedNodeId(3)));

        testGraph(graphStore, sourceNodes, targetNodes, 194, 31_459);


        sourceNodes = new MapInputNodes(Map.of(idMap.toMappedNodeId(3), 80D, idMap.toMappedNodeId(8), 93D));
        targetNodes = new MapInputNodes(Map.of(idMap.toMappedNodeId(5), 54D, idMap.toMappedNodeId(19), 119D));

        testGraph(graphStore, sourceNodes, targetNodes, 173, 16_740);
    }

}
