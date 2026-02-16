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
package org.neo4j.gds.paths.yens;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
public class PeekPruningTest {
    @GdlGraph(indexInverse = true)
    private static final String DB_CYPHER =
        "CREATE" +
            "  (c:C {id: 0})" +
            ", (d:D {id: 1})" +
            ", (e:E {id: 2})" +
            ", (f:F {id: 3})" +
            ", (g:G {id: 4})" +
            ", (h:H {id: 5})" +
            ", (i:I {id: 6})" +
            ", (z:Z {id: 7})" +
            ", (c)-[:REL {cost: 3.0}]->(d)" +
            ", (c)-[:REL {cost: 2.0}]->(e)" +
            ", (c)-[:REL {cost: 4.0}]->(i)" +
            ", (d)-[:REL {cost: 4.0}]->(f)" +
            ", (e)-[:REL {cost: 1.0}]->(d)" +
            ", (e)-[:REL {cost: 2.0}]->(f)" +
            ", (e)-[:REL {cost: 3.0}]->(g)" +
            ", (f)-[:REL {cost: 2.0}]->(g)" +
            ", (f)-[:REL {cost: 1.0}]->(h)" +
            ", (g)-[:REL {cost: 2.0}]->(h)" +
            ", (i)-[:REL {cost: 4.0}]->(h)";

    @Inject
    private TestGraph graph;

    @Test
    void testPruning() {
        long nodeCount = graph.nodeCount();
        String[] originalNames = new String[]{"c", "d", "e", "f", "g", "h", "i", "z"};
        String[] names = new String[(int)nodeCount];
        for (var name : originalNames) {
            names[(int)graph.toMappedNodeId(name)] = name;
        }
        long sourceNode = graph.toMappedNodeId("c");
        double cutoff = 7.0;
        var nodeIncluded = new boolean[]{false, true, false, true, true, true, true, false};
        var forwardDistance = new double[]{0.0, 0.0, 3.0, 2.0, 4.0, 5.0, 5.0, 4.0};
        var backwardDistance = new double[]{0.0, 5.0, 5.0, 3.0, 1.0, 2.0, 0.0, 4.0};
        var filtered = PeekPruning.createPrunedGraph(
            graph,
            sourceNode,
            cutoff,
            n -> forwardDistance[(int)n],
            n-> backwardDistance[(int)n],
            n -> nodeIncluded[(int)n],
            new Concurrency(1));
        assertThat(filtered.nodeCount()).isEqualTo(5);
        assertThat(filtered.toMappedNodeId(graph.toMappedNodeId("i"))).isEqualTo(IdMap.NOT_FOUND);
        assertThat(filtered.toMappedNodeId(graph.toMappedNodeId("c"))).isNotEqualTo(IdMap.NOT_FOUND);
        Map<String, Set<String>> expectedGraph = Map.of(
            "c", Set.of("e"),
            "e", Set.of("f", "g"),
            "f", Set.of("h"),
            "g", Set.of("h"),
            "h", Set.of()
        );
        Function<Long, String> originalName = (node) -> names[(int)filtered.toOriginalNodeId(node)];
        filtered.forEachNode(node -> {
            var set = new HashSet<String>();
            filtered.forEachRelationship(node, (source, target) -> {
                set.add(originalName.apply(target));
                return true;
            });
            String key = originalName.apply(node);
            assertThat(expectedGraph).containsKey(key);
            assertThat(set).containsExactlyInAnyOrder(expectedGraph.get(key).toArray(String[]::new));
            return true;
        });
    }

    @Test
    void testReachability() {
        var concurrency = new Concurrency(1);
        var executorService = DefaultPool.INSTANCE;
        var terminationFlag = TerminationFlag.RUNNING_TRUE;

        BiFunction<String, String, Long> reachableFrom = (source, target) ->
            PeekPruning.pathsAndReachability(graph, graph.toMappedNodeId(source), graph.toMappedNodeId(target), PeekPruning.deltaStep(concurrency, executorService, terminationFlag))
                .reachable().cardinality();

        assertThat(reachableFrom.apply("c", "z")).isEqualTo(0);
        assertThat(reachableFrom.apply("c", "h")).isEqualTo(7);
        assertThat(reachableFrom.apply("h", "c")).isEqualTo(0);
        assertThat(reachableFrom.apply("d", "h")).isEqualTo(4);
    }

    @Test
    void testSorting() {
        var nodeIncluded = new boolean[]{false, true, false, true, true, true, true, false};
        var forwardDistance = new double[]{0.0, 0.0, 3.0, 2.5, 4.0, 5.0, 3.0, 4.5};
        var backwardDistance = new double[]{0.0, 4.0, 5.0, 3.0, 1.0, 2.0, 1.5, 4.0};
        var combinedPaths = PeekPruning.sortedCombinedPathCosts(8, 5, n -> nodeIncluded[(int)n], n -> forwardDistance[(int)n], n -> backwardDistance[(int)n]);
        DoubleStream.Builder costs = DoubleStream.builder();
        LongStream.Builder indices = LongStream.builder();
        for (int n = 0; n < combinedPaths.paths().size(); n++) {
            var path = combinedPaths.paths().get(n);
            costs.add(path.value());
            indices.add(path.index());
        }
        assertThat(costs.build().toArray()).isEqualTo(new double[]{4.0, 4.5, 5.0, 5.5, 7.0});
        assertThat(indices.build().toArray()).isEqualTo(new long[]{1, 6, 4, 3, 5});
    }

    @Test
    void testEndToEnd() {
        var nodeCount = 10000L;
        var randomGraph = new RandomGraphGeneratorBuilder()
            .seed(6)
            .nodeCount(nodeCount)
            .averageDegree(10)
            .inverseIndex(true)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .relationshipPropertyProducer(PropertyProducer.randomDouble("weight", 1.0, 20.0))
            .build().generate();

        int k = 5;
        long sourceNode = 1;
        long targetNode = nodeCount-1;
        var concurrency = new Concurrency(1);
        var executorService = DefaultPool.INSTANCE;
        var terminationFlag = TerminationFlag.RUNNING_TRUE;

        var yen = Yens.sourceTarget(
            randomGraph,
            new YensParameters(
                sourceNode,
                targetNode,
                k,
                concurrency),
            ProgressTracker.NULL_TRACKER,
            terminationFlag).compute();

        var prunedYen = new PeekPruningYens(
            randomGraph,
            sourceNode,
            targetNode,
            k,
            concurrency,
            executorService,
            ProgressTracker.NULL_TRACKER,
            terminationFlag).compute();

        var yenPaths = yen.paths().toArray(PathResult[]::new);
        var prunedPaths = prunedYen.paths().toArray(PathResult[]::new);
        assertThat(Arrays.stream(yenPaths).mapToLong(PathResult::sourceNode).toArray()).containsExactly(Arrays.stream(prunedPaths).mapToLong(PathResult::sourceNode).toArray());
        assertThat(Arrays.stream(yenPaths).mapToLong(PathResult::targetNode).toArray()).containsExactly(Arrays.stream(prunedPaths).mapToLong(PathResult::targetNode).toArray());
        assertThat(Arrays.stream(yenPaths).mapToDouble(PathResult::totalCost).toArray()).containsExactly(Arrays.stream(prunedPaths).mapToDouble(PathResult::totalCost).toArray());
        assertThat(Arrays.stream(yenPaths).map(PathResult::nodeIds).toArray(long[][]::new)).isEqualTo(Arrays.stream(prunedPaths).map(PathResult::nodeIds).toArray(long[][]::new));
        assertThat(Arrays.stream(yenPaths).map(PathResult::costs).toArray(double[][]::new)).isEqualTo(Arrays.stream(prunedPaths).map(PathResult::costs).toArray(double[][]::new));
    }
}
