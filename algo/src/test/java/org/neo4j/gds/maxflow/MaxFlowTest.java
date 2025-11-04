
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
package org.neo4j.gds.maxflow;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.InputNodes;
import org.neo4j.gds.ListInputNodes;
import org.neo4j.gds.MapInputNodes;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.beta.generator.RelationshipDistribution.POWER_LAW;
import static org.neo4j.gds.beta.generator.RelationshipDistribution.UNIFORM;

class MaxFlowTest {
    private static final double TOLERANCE = 1e-6;

    Graph generate(long nodeCount, int avgDegree, RelationshipDistribution relDistr) {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(nodeCount)
            .averageDegree(avgDegree)
            .relationshipDistribution(relDistr)
            .relationshipPropertyProducer(PropertyProducer.randomDouble("capacity", 0, 100))
            .seed(0)
            .build()
            .generate();
        return graph;
    }

    void testGraph(Graph graph, InputNodes sourceNodes, InputNodes targetNodes, double expectedFlow, int concurrency) {
        var params = new MaxFlowParameters(sourceNodes, targetNodes, new Concurrency(concurrency), .5, true);
        var x = new MaxFlow(graph, params, ProgressTracker.NULL_TRACKER, TerminationFlag.RUNNING_TRUE);
        var start = System.nanoTime();
        var result = x.compute();
        System.out.println("Compute time: " + (System.nanoTime() - start) / 1_000_000 + "ms");
        assertThat(result.totalFlow()).isCloseTo(expectedFlow, Offset.offset(TOLERANCE));
    }

    void testGraph(Graph graph, long sourceNode, long targetNode, double expectedFlow, int concurrency) {
        var sourceNodes = new ListInputNodes(List.of(sourceNode));
        var targetNodes = new ListInputNodes(List.of(targetNode));

        testGraph(graph, sourceNodes, targetNodes, expectedFlow, concurrency);
    }

    void testGraph(TestGraph graph, String sourceNode, String targetNode, double expectedFlow) {
        testGraph(graph.graph(), graph.toOriginalNodeId(sourceNode), graph.toOriginalNodeId(targetNode), expectedFlow, 1);
    }

    @Test
    void testA() {
        var graph = fromGdl("""
            CREATE
                (a:Node {id: 0}),
                (b:Node {id: 1}),
                (c:Node {id: 2}),
                (d:Node {id: 3}),
                (e:Node {id: 4}),
                (a)-[:R {w: 4.0}]->(d),
                (b)-[:R {w: 3.0}]->(a),
                (c)-[:R {w: 2.0}]->(a),
                (c)-[:R {w: 0.0}]->(b),
                (d)-[:R {w: 5.0}]->(e)
            """);

        testGraph(graph, "a", "e", 4.0);
    }

    @Test
    void testB() {
        var graph = fromGdl("""
            CREATE
            (a0),(a1),(a3),(a4),(a5),(a6),
              (a0)-[:R{w:5.0}]->(a2),
              (a0)-[:R{w:6.0}]->(a5),
              (a0)-[:R{w:7.0}]->(a6),
              (a1)-[:R{w:9.0}]->(a2),
              (a1)-[:R{w:7.0}]->(a3),
              (a2)-[:R{w:9.0}]->(a0),
              (a2)-[:R{w:12.0}]->(a6),
              (a3)-[:R{w:11.0}]->(a2),
              (a3)-[:R{w:4.0}]->(a4),
              (a4)-[:R{w:7.0}]->(a0),
              (a4)-[:R{w:8.0}]->(a3),
              (a4)-[:R{w:6.0}]->(a6),
              (a5)-[:R{w:1.0}]->(a0),
              (a5)-[:R{w:1.0}]->(a1),
              (a5)-[:R{w:8.0}]->(a6),
              (a6)-[:R{w:3.0}]->(a1),
              (a6)-[:R{w:8.0}]->(a5)
            """);

        testGraph(graph, "a2", "a0", 14.0);
    }


    @Test
    void test1() {
        var graph = fromGdl("""
            CREATE
            (a0),
            (a1),
            (a2),
            (a3),
            (a4),
            (a0)-[:R {u: 1.0}]->(a2),
            (a0)-[:R {u: 7.0}]->(a3),
            (a1)-[:R {u: 7.0}]->(a2),
            (a1)-[:R {u: 2.0}]->(a3),
            (a2)-[:R {u: 10.0}]->(a3),
            (a3)-[:R {u: 3.0}]->(a0),
            (a3)-[:R {u: 9.0}]->(a4),
            (a4)-[:R {u: 4.0}]->(a0),
            (a4)-[:R {u: 8.0}]->(a1)
            """, Orientation.NATURAL
        );

        testGraph(graph, "a3", "a2", 8.0);
    }

    @Test
    void test2a() {
        var graph = fromGdl(
            """
                CREATE
                    (a0)-[:R {capacity: 91}]->(a1),
                    (a0)-[:R {capacity: 19}]->(a2),
                    (a0)-[:R {capacity: 13}]->(a3),
                    (a1)-[:R {capacity: 46}]->(a2),
                    (a1)-[:R {capacity: 41}]->(a3),
                    (a2)-[:R {capacity: 71}]->(a3)
                """,
            Orientation.NATURAL
        );

        testGraph(graph, "a0", "a3", 119.0);
    }

    @Test
    void test2() {
        var graph = fromGdl(
            """
                CREATE
                    (a0)-[:R {capacity: 50}]->(a5),
                    (a0)-[:R {capacity: 54}]->(a8),
                    (a0)-[:R {capacity: 34}]->(a9),
                    (a1)-[:R {capacity: 63}]->(a5),
                    (a1)-[:R {capacity: 39}]->(a6),
                    (a1)-[:R {capacity: 46}]->(a7),
                    (a1)-[:R {capacity: 28}]->(a8),
                    (a1)-[:R {capacity: 18}]->(a9),
                    (a2)-[:R {capacity: 18}]->(a5),
                    (a2)-[:R {capacity: 13}]->(a6),
                    (a2)-[:R {capacity: 33}]->(a7),
                    (a3)-[:R {capacity: 91}]->(a5),
                    (a3)-[:R {capacity: 19}]->(a6),
                    (a3)-[:R {capacity: 13}]->(a7),
                    (a4)-[:R {capacity: 10}]->(a5),
                    (a4)-[:R {capacity: 43}]->(a6),
                    (a4)-[:R {capacity: 72}]->(a8),
                    (a5)-[:R {capacity: 46}]->(a6),
                    (a5)-[:R {capacity: 41}]->(a7),
                    (a5)-[:R {capacity: 82}]->(a9),
                    (a6)-[:R {capacity: 71}]->(a7),
                    (a6)-[:R {capacity: 57}]->(a8),
                    (a6)-[:R {capacity: 34}]->(a9),
                    (a7)-[:R {capacity: 71}]->(a8),
                    (a7)-[:R {capacity: 12}]->(a9)
                """,
            Orientation.NATURAL
        );

        testGraph(graph, "a3", "a7", 119.0);
    }

    @Test
    void test3() {
        var graph = fromGdl(
            """
                CREATE
                    (a0)-[:R {capacity: 50}]->(a10),
                    (a0)-[:R {capacity: 54}]->(a11),
                    (a0)-[:R {capacity: 34}]->(a13),
                    (a0)-[:R {capacity: 63}]->(a14),
                    (a0)-[:R {capacity: 39}]->(a15),
                    (a0)-[:R {capacity: 46}]->(a17),
                    (a0)-[:R {capacity: 28}]->(a18),
                    (a0)-[:R {capacity: 18}]->(a21),
                    (a0)-[:R {capacity: 18}]->(a22),
                    (a0)-[:R {capacity: 13}]->(a23),
                    (a1)-[:R {capacity: 33}]->(a10),
                    (a1)-[:R {capacity: 91}]->(a11),
                    (a1)-[:R {capacity: 19}]->(a12),
                    (a1)-[:R {capacity: 13}]->(a13),
                    (a1)-[:R {capacity: 10}]->(a18),
                    (a1)-[:R {capacity: 43}]->(a19),
                    (a1)-[:R {capacity: 72}]->(a21),
                    (a1)-[:R {capacity: 46}]->(a22),
                    (a2)-[:R {capacity: 41}]->(a10),
                    (a2)-[:R {capacity: 82}]->(a12),
                    (a2)-[:R {capacity: 71}]->(a15),
                    (a2)-[:R {capacity: 57}]->(a20),
                    (a3)-[:R {capacity: 34}]->(a10),
                    (a3)-[:R {capacity: 71}]->(a11),
                    (a3)-[:R {capacity: 12}]->(a13),
                    (a3)-[:R {capacity: 52}]->(a17),
                    (a3)-[:R {capacity: 86}]->(a18),
                    (a3)-[:R {capacity: 1}]->(a20),
                    (a3)-[:R {capacity: 64}]->(a23),
                    (a4)-[:R {capacity: 32}]->(a10),
                    (a4)-[:R {capacity: 42}]->(a11),
                    (a4)-[:R {capacity: 9}]->(a12),
                    (a4)-[:R {capacity: 73}]->(a13),
                    (a4)-[:R {capacity: 31}]->(a14),
                    (a4)-[:R {capacity: 70}]->(a15),
                    (a4)-[:R {capacity: 12}]->(a17),
                    (a4)-[:R {capacity: 41}]->(a18),
                    (a4)-[:R {capacity: 63}]->(a22),
                    (a4)-[:R {capacity: 39}]->(a23),
                    (a4)-[:R {capacity: 38}]->(a24),
                    (a5)-[:R {capacity: 16}]->(a10),
                    (a5)-[:R {capacity: 43}]->(a11),
                    (a5)-[:R {capacity: 27}]->(a12),
                    (a5)-[:R {capacity: 71}]->(a14),
                    (a5)-[:R {capacity: 37}]->(a15),
                    (a5)-[:R {capacity: 12}]->(a16),
                    (a5)-[:R {capacity: 50}]->(a17),
                    (a5)-[:R {capacity: 74}]->(a24),
                    (a6)-[:R {capacity: 38}]->(a10),
                    (a6)-[:R {capacity: 25}]->(a11),
                    (a6)-[:R {capacity: 5}]->(a12),
                    (a6)-[:R {capacity: 85}]->(a13),
                    (a6)-[:R {capacity: 61}]->(a14),
                    (a6)-[:R {capacity: 12}]->(a19),
                    (a6)-[:R {capacity: 97}]->(a22),
                    (a6)-[:R {capacity: 20}]->(a24),
                    (a7)-[:R {capacity: 11}]->(a10),
                    (a7)-[:R {capacity: 70}]->(a11),
                    (a7)-[:R {capacity: 51}]->(a12),
                    (a7)-[:R {capacity: 68}]->(a13),
                    (a7)-[:R {capacity: 67}]->(a14),
                    (a7)-[:R {capacity: 28}]->(a15),
                    (a7)-[:R {capacity: 76}]->(a16),
                    (a7)-[:R {capacity: 75}]->(a19),
                    (a7)-[:R {capacity: 58}]->(a24),
                    (a8)-[:R {capacity: 85}]->(a10),
                    (a8)-[:R {capacity: 90}]->(a11),
                    (a8)-[:R {capacity: 11}]->(a12),
                    (a8)-[:R {capacity: 79}]->(a13),
                    (a8)-[:R {capacity: 63}]->(a15),
                    (a8)-[:R {capacity: 81}]->(a16),
                    (a8)-[:R {capacity: 25}]->(a19),
                    (a8)-[:R {capacity: 3}]->(a23),
                    (a9)-[:R {capacity: 35}]->(a10),
                    (a9)-[:R {capacity: 91}]->(a11),
                    (a9)-[:R {capacity: 48}]->(a12),
                    (a9)-[:R {capacity: 43}]->(a13),
                    (a9)-[:R {capacity: 8}]->(a14),
                    (a9)-[:R {capacity: 19}]->(a15),
                    (a9)-[:R {capacity: 29}]->(a16),
                    (a9)-[:R {capacity: 74}]->(a17),
                    (a9)-[:R {capacity: 69}]->(a21),
                    (a9)-[:R {capacity: 88}]->(a22),
                    (a9)-[:R {capacity: 4}]->(a23),
                    (a9)-[:R {capacity: 82}]->(a24),
                    (a10)-[:R {capacity: 78}]->(a11),
                    (a10)-[:R {capacity: 16}]->(a12),
                    (a10)-[:R {capacity: 12}]->(a13),
                    (a10)-[:R {capacity: 15}]->(a14),
                    (a10)-[:R {capacity: 78}]->(a15),
                    (a10)-[:R {capacity: 25}]->(a16),
                    (a10)-[:R {capacity: 92}]->(a18),
                    (a10)-[:R {capacity: 62}]->(a20),
                    (a10)-[:R {capacity: 94}]->(a21),
                    (a10)-[:R {capacity: 87}]->(a23),
                    (a10)-[:R {capacity: 70}]->(a24),
                    (a11)-[:R {capacity: 80}]->(a12),
                    (a11)-[:R {capacity: 34}]->(a14),
                    (a11)-[:R {capacity: 29}]->(a15),
                    (a11)-[:R {capacity: 83}]->(a16),
                    (a11)-[:R {capacity: 45}]->(a17),
                    (a11)-[:R {capacity: 24}]->(a18),
                    (a11)-[:R {capacity: 65}]->(a19),
                    (a11)-[:R {capacity: 6}]->(a20),
                    (a11)-[:R {capacity: 13}]->(a21),
                    (a11)-[:R {capacity: 51}]->(a23),
                    (a12)-[:R {capacity: 34}]->(a13),
                    (a12)-[:R {capacity: 94}]->(a14),
                    (a12)-[:R {capacity: 73}]->(a15),
                    (a12)-[:R {capacity: 90}]->(a16),
                    (a12)-[:R {capacity: 27}]->(a17),
                    (a12)-[:R {capacity: 8}]->(a18),
                    (a12)-[:R {capacity: 21}]->(a20),
                    (a12)-[:R {capacity: 44}]->(a21),
                    (a12)-[:R {capacity: 33}]->(a22),
                    (a12)-[:R {capacity: 77}]->(a23),
                    (a13)-[:R {capacity: 86}]->(a14),
                    (a13)-[:R {capacity: 2}]->(a16),
                    (a13)-[:R {capacity: 88}]->(a17),
                    (a13)-[:R {capacity: 73}]->(a18),
                    (a13)-[:R {capacity: 40}]->(a21),
                    (a13)-[:R {capacity: 46}]->(a22),
                    (a13)-[:R {capacity: 85}]->(a24),
                    (a14)-[:R {capacity: 20}]->(a18),
                    (a14)-[:R {capacity: 89}]->(a19),
                    (a14)-[:R {capacity: 59}]->(a20),
                    (a14)-[:R {capacity: 11}]->(a21),
                    (a15)-[:R {capacity: 95}]->(a17),
                    (a15)-[:R {capacity: 70}]->(a18),
                    (a15)-[:R {capacity: 18}]->(a19),
                    (a15)-[:R {capacity: 98}]->(a21),
                    (a16)-[:R {capacity: 46}]->(a17),
                    (a16)-[:R {capacity: 37}]->(a19),
                    (a16)-[:R {capacity: 46}]->(a20),
                    (a16)-[:R {capacity: 82}]->(a22),
                    (a16)-[:R {capacity: 17}]->(a23),
                    (a17)-[:R {capacity: 40}]->(a19),
                    (a17)-[:R {capacity: 96}]->(a20),
                    (a17)-[:R {capacity: 84}]->(a21),
                    (a17)-[:R {capacity: 1}]->(a24),
                    (a18)-[:R {capacity: 25}]->(a19),
                    (a18)-[:R {capacity: 43}]->(a20),
                    (a18)-[:R {capacity: 31}]->(a22),
                    (a18)-[:R {capacity: 82}]->(a23),
                    (a18)-[:R {capacity: 49}]->(a24),
                    (a19)-[:R {capacity: 87}]->(a20),
                    (a19)-[:R {capacity: 54}]->(a22),
                    (a19)-[:R {capacity: 52}]->(a24)
                """,
            Orientation.NATURAL
        );

        testGraph(graph, "a0", "a15", 143.0);
    }

    @Test
    void test4() {
        var graph = generate(200L, 10, UNIFORM);
        testGraph(graph, 50, 100, 434.3606561583014, 1);

        testGraph(graph,
            new MapInputNodes(Map.of(1L, 103.1, 23L, 129.5, 101L, 242.2)),
            new MapInputNodes(Map.of(5L, 117.7, 199L, 199.0, 150L, 204.5)),
            474.8,
            1);
    }

    @Test
    void test5() {
        var graph = generate(1000L, 25, UNIFORM);
        testGraph(graph, 100, 200, 1091.5727039914948, 1);


        testGraph(graph,
            new MapInputNodes(Map.of(1L, 100.1, 23L, 120.5, 501L, 142.2)),
            new MapInputNodes(Map.of(5L, 157.7, 299L, 109.0, 450L, 204.5)),
            362.79999999999995,
            4);
    }

    @Test
    void test6a() {
        var graph = generate(10_000L, 100, UNIFORM);
        testGraph(graph, 1_234, 5_678, 4470.905417520765, 1);
    }

    @Test
    void test6b() {
        var graph = generate(500_000L, 50, UNIFORM);
        testGraph(graph, 123_456, 234_567, 1810.4604332732595, 8);
    }

    @Test
    void test6c() {
        var graph = generate(1_000_000L, 100, UNIFORM);
        testGraph(graph, 123_456, 234_567, 4862.678345610048, 8);
    }

    @Test
    void test6d() {
        var graph = generate(3_000_000L, 100, UNIFORM);
        testGraph(graph, 1_234_567, 2_345_678, 4761.718858079709, 8);
    }

    @Test
    void test7a() {
        var graph = generate(10_000L, 25, POWER_LAW);
        testGraph(graph, 100, 200, 349.99795684746823, 1);
    }

    @Test
    void test7b() {
        var graph = generate(100_000L, 10, POWER_LAW);
        testGraph(graph, 100, 200, 394.0392308340888, 1);
    }

    @Test
    void test7c() {
        var graph = generate(1_000_000L, 50, POWER_LAW);
        testGraph(graph, 100, 200, 887.1128713190556, 8);
    }

    @Test
    void shouldLogProgress() {
        var graph = generate(100L, 10, UNIFORM);
        var log = new GdsTestLog();
        var testTracker = TestProgressTracker.create(
            MaxFlowTask.create(),
            new LoggerForProgressTrackingAdapter(log),
            new Concurrency(4),
            EmptyTaskRegistryFactory.INSTANCE
        );

        new MaxFlow(
            graph,
            new MaxFlowParameters(
                new ListInputNodes(List.of(0L)),
                new ListInputNodes(List.of(2L)),
                new Concurrency(4),
                .5D,
                true
            ),
            testTracker,
            TerminationFlag.RUNNING_TRUE
        ).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "MaxFlow :: Start",
                "MaxFlow 7%",
                "MaxFlow 40%",
                "MaxFlow 74%",
                "MaxFlow 87%",
                "MaxFlow 91%",
                "MaxFlow 99%",
                "MaxFlow 100%",
                "MaxFlow :: Finished"
            );
    }
}
