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
package org.neo4j.gds.paths.bellmanford;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.paths.delta.config.ImmutableAllShortestPathsDeltaStreamConfig;
import org.neo4j.gds.paths.dijkstra.Dijkstra;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class BellmanFordTest {
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0)," +
        "  (a1)," +
        "  (a2)," +
        "  (a3)," +
        "  (a4)," +
        "  (a0)-[:R {weight:  1.0}]->(a1)," +
        "  (a0)-[:R {weight: -1.0}]->(a2)," +
        "  (a0)-[:R {weight: 10.0}]->(a3)," +
        "  (a3)-[:R {weight: -8.0}]->(a4)," +
        "  (a1)-[:R {weight:  3.0}]->(a4) ";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @GdlGraph(graphNamePrefix = "loop")
    private static final String LOOP_DB_CYPHER =
        "CREATE " +
        "  (a0)," +
        "  (a1)," +
        "  (a2)," +
        "  (a3)," +
        "  (a4)," +
        "  (a0)-[:R {weight:  1.0}]->(a1)," +
        "  (a0)-[:R {weight: 10.0}]->(a2)," +
        "  (a2)-[:R {weight: -8.0}]->(a3)," +
        "  (a3)-[:R {weight: -4.0}]->(a4)," +
        "  (a4)-[:R {weight:  1.0}]->(a2) ";

    @Inject
    private TestGraph loopGraph;

    @Inject
    private IdFunction loopIdFunction;

    // another graph: https://www.javatpoint.com/bellman-ford-algorithm
    @GdlGraph(graphNamePrefix = "third")
    private static final String EXAMPLE_2_DB_CYPHER =
        "CREATE " +
        "  (A)," +
        "  (B)," +
        "  (C)," +
        "  (D)," +
        "  (E)," +
        "  (F)," +
        "  (L)," +
        "  (A)-[:R {weight:  6.0}]->(B)," +
        "  (A)-[:R {weight:  4.0}]->(C)," +
        "  (A)-[:R {weight:  5.0}]->(D)," +
        "  (B)-[:R {weight: -1.0}]->(E)," +
        "  (C)-[:R {weight: -2.0}]->(B)," +
        "  (C)-[:R {weight:  3.0}]->(E)," +
        "  (D)-[:R {weight: -2.0}]->(C)," +
        "  (D)-[:R {weight: -1.0}]->(F)," +
        "  (E)-[:R {weight:  3.0}]->(F) ";

    @Inject
    private TestGraph thirdGraph;

    @Inject
    private IdFunction thirdIdFunction;

    @Test
    void shouldComputeShortestPathsWithoutLoops() {
        long[] a = new long[]{
            idFunction.of("a0"),
            idFunction.of("a1"),
            idFunction.of("a2"),
            idFunction.of("a3"),
            idFunction.of("a4")
        };
        var result = new BellmanFord(graph, ProgressTracker.NULL_TRACKER, a[0], 1).compute();
        long[][] EXPECTED_PATHS = new long[5][];
        EXPECTED_PATHS[(int) a[0]] = new long[]{a[0]};
        EXPECTED_PATHS[(int) a[1]] = new long[]{a[0], a[1]};
        EXPECTED_PATHS[(int) a[2]] = new long[]{a[0], a[2]};
        EXPECTED_PATHS[(int) a[3]] = new long[]{a[0], a[3]};
        EXPECTED_PATHS[(int) a[4]] = new long[]{a[0], a[3], a[4]};
        double[] EXPECTED_COSTS = new double[5];
        EXPECTED_COSTS[(int) a[0]] = 0;
        EXPECTED_COSTS[(int) a[1]] = 1;
        EXPECTED_COSTS[(int) a[2]] = -1;
        EXPECTED_COSTS[(int) a[3]] = 10;
        EXPECTED_COSTS[(int) a[4]] = 2;

        long counter = 0;
        for (var path : result.shortestPaths().pathSet()) {
            counter++;
            int currentTargetNode = (int) path.targetNode();
            assertThat(path.nodeIds()).isEqualTo(EXPECTED_PATHS[currentTargetNode]);
            assertThat(EXPECTED_COSTS[currentTargetNode]).isEqualTo(path.totalCost());
        }
        assertThat(counter).isEqualTo(5L);
        assertThat(result.containsNegativeCycle()).isFalse();
    }

    @Test
    void shouldStopAtLoops() {
        long a0 = loopIdFunction.of("a0");
        var result = new BellmanFord(loopGraph, ProgressTracker.NULL_TRACKER, a0, 1).compute();
        int counter = 0;
        for (var path : result.shortestPaths().pathSet()) {
            counter++;
        }
        assertThat(result.containsNegativeCycle()).isTrue();
        assertThat(counter).isEqualTo(0L);
    }

    @Test
    void shouldUpdateBasedOnNegativeCorrectly() {
        long[] nodes = new long[]{
            thirdIdFunction.of("A"),
            thirdIdFunction.of("B"),
            thirdIdFunction.of("C"),
            thirdIdFunction.of("D"),
            thirdIdFunction.of("E"),
            thirdIdFunction.of("F")};

        var result = new BellmanFord(thirdGraph, ProgressTracker.NULL_TRACKER, nodes[0], 1).compute();
        long[][] EXPECTED_PATHS = new long[6][];
        EXPECTED_PATHS[(int) nodes[0]] = new long[]{nodes[0]};
        EXPECTED_PATHS[(int) nodes[1]] = new long[]{nodes[0], nodes[3], nodes[2], nodes[1]};
        EXPECTED_PATHS[(int) nodes[2]] = new long[]{nodes[0], nodes[3], nodes[2]};
        EXPECTED_PATHS[(int) nodes[3]] = new long[]{nodes[0], nodes[3]};
        EXPECTED_PATHS[(int) nodes[4]] = new long[]{nodes[0], nodes[3], nodes[2], nodes[1], nodes[4]};
        EXPECTED_PATHS[(int) nodes[5]] = new long[]{nodes[0], nodes[3], nodes[2], nodes[1], nodes[4], nodes[5]};

        double[] EXPECTED_COSTS = new double[6];
        EXPECTED_COSTS[(int) nodes[0]] = 0;
        EXPECTED_COSTS[(int) nodes[1]] = 1;
        EXPECTED_COSTS[(int) nodes[2]] = 3;
        EXPECTED_COSTS[(int) nodes[3]] = 5;
        EXPECTED_COSTS[(int) nodes[4]] = 0;
        EXPECTED_COSTS[(int) nodes[5]] = 3;

        long counter = 0;
        for (var path : result.shortestPaths().pathSet()) {
            counter++;
            int currentTargetNode = (int) path.targetNode();
            assertThat(path.nodeIds()).isEqualTo(EXPECTED_PATHS[currentTargetNode]);
            assertThat(EXPECTED_COSTS[currentTargetNode]).isEqualTo(path.totalCost());
        }
        assertThat(counter).isEqualTo(6L);
        assertThat(result.containsNegativeCycle()).isFalse();
    }
    
    @Test
    void shouldGiveSameResultsAsDijkstra() {
        int nodeCount = 3_000;
        long seed = 42L;
        long start = 42;
        int concurrency = 4;
        var newGraph = new RandomGraphGeneratorBuilder()
            .direction(Direction.DIRECTED)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .relationshipPropertyProducer(PropertyProducer.randomDouble("foo", 1, 10))
            .nodeCount(nodeCount)
            .seed(seed)
            .build()
            .generate();

        var config = ImmutableAllShortestPathsDeltaStreamConfig.builder()
            .concurrency(concurrency)
            .sourceNode(start)
            .trackRelationships(true)
            .build();

        var bellmanFord = new BellmanFord(
            newGraph,
            ProgressTracker.NULL_TRACKER,
            start,
            4
        ).compute().shortestPaths();

        var dijkstraAlgo = Dijkstra
            .singleSource(newGraph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
            .compute();

        double[] bellman = new double[nodeCount];
        double[] djikstra = new double[nodeCount];

        double bellmanSum = 0;
        double dijkstraSum = 0;

        for (var path : bellmanFord.pathSet()) {
            bellman[(int) path.targetNode()] = path.totalCost();
        }

        for (var path : dijkstraAlgo.pathSet()) {
            djikstra[(int) path.targetNode()] = path.totalCost();
        }
        for (int i = 0; i < nodeCount; ++i) {
            bellmanSum += bellman[i];
            dijkstraSum += djikstra[i];
            assertThat(djikstra[i]).isCloseTo(bellman[i], Offset.offset(1e-5));

        }
        assertThat(bellmanSum).isCloseTo(dijkstraSum, Offset.offset(1e-5));
    }
}
