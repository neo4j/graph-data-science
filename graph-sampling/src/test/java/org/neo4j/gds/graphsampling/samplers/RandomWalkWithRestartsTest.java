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
package org.neo4j.gds.graphsampling.samplers;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfigImpl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class RandomWalkWithRestartsTest {

    @GdlGraph(idOffset = 42)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (x:Z {prop: 42})" +
        ", (x1:Z {prop: 43})" +
        ", (x2:Z {prop: 44})" +
        ", (x3:Z {prop: 45})" +
        ", (a:N {prop: 46})" +
        ", (b:N {prop: 47})" +
        ", (c:N {prop: 48, attr: 48})" +
        ", (d:N {prop: 49, attr: 48})" +
        ", (e:M {prop: 50, attr: 48})" +
        ", (f:M {prop: 51, attr: 48})" +
        ", (g:M {prop: 52})" +
        ", (h:M {prop: 53})" +
        ", (i:X {prop: 54})" +
        ", (j:M {prop: 55})" +
        ", (x)-[:R1 {distance: 0.0} ]->(x1)" +
        ", (x)-[:R1 {distance: 2.0} ]->(x2)" +
        ", (x)-[:R1 {distance: 200.0} ]->(x3)" +
        ", (e)-[:R1 {distance: 1.0} ]->(d)" +
        ", (i)-[:R1 {distance: 1.0} ]->(g)" +
        ", (a)-[:R1 {cost: 10.0, distance: 5.8}]->(b)" +
        ", (a)-[:R1 {cost: 10.0, distance: 4.8}]->(c)" +
        ", (c)-[:R1 {cost: 10.0, distance: 5.8}]->(d)" +
        ", (d)-[:R1 {cost:  4.2, distance: 2.6}]->(e)" +
        ", (e)-[:R1 {cost: 10.0, distance: 5.8}]->(f)" +
        ", (f)-[:R1 {cost: 10.0, distance: 9.9}]->(g)" +
        ", (h)-[:R2 {cost: 10.0, distance: 5.8}]->(i)";

    @Inject
    private GraphStore graphStore;

    Graph getGraph(RandomWalkWithRestartsConfig config) {
        return graphStore.getGraph(
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore),
            config.relationshipWeightProperty()
        );
    }

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldSample() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNodes(List.of(idFunction.of("a")))
            .samplingRatio(0.5)
            .restartProbability(0.1)
            .build();

        var rwr = new RandomWalkWithRestarts(config);
        var graph = getGraph(config);
        var nodes = rwr.compute(graph, ProgressTracker.NULL_TRACKER);

        assertThat(nodes.cardinality()).isEqualTo(7);

        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("a")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("b")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("c")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("d")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("e")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("f")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("g")))).isTrue();
    }

    @Test
    void shouldSampleWeighted() {
        double casesPassed = 0;
        var validCases = 0;
        for (long seed = 0; seed < 250; seed++) {
            var config = RandomWalkWithRestartsConfigImpl.builder()
                .startNodes(List.of(idFunction.of("x")))
                .relationshipWeightProperty("distance")
                .samplingRatio(0.22)
                .restartProbability(0.01)
                .randomSeed(seed)
                .concurrency(1)
                .build();

            var graph = getGraph(config);
            var rwr = new RandomWalkWithRestarts(config);
            var nodes = rwr.compute(graph, ProgressTracker.NULL_TRACKER);
            if (rwr.startNodesUsed().contains(idFunction.of("x1")) || rwr
                .startNodesUsed()
                .contains(idFunction.of("x2"))) {
                continue;
            }
            validCases++;

            assertThat(nodes.cardinality()).isEqualTo(3L);
            assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("x1")))).isFalse();

            if (nodes.get(graph.toMappedNodeId(idFunction.of("x"))) &&
                !nodes.get(graph.toMappedNodeId(idFunction.of("x2"))) &&
                nodes.get(graph.toMappedNodeId(idFunction.of("x3")))
            ) {
                casesPassed++;
            }
        }
        // the probability that we walk from x to x3 everytime until a new startnode is picked
        // is P(x->x3) ^ <number of walks until new startnode given that x3 is picked every time>
        // the number of these walks is 30, because first walk keeps quality at 1 and for remaining
        // walks we have quality *= 0.9 and 0.9 ^ 29 is the first that is lower than the threshold 0.05.
        // therefore the probability of a case passing is (200 / 202) ^ 30.
        assertThat(casesPassed / validCases).isCloseTo(Math.pow(200.0 / 202, 30), Offset.offset(0.015));
    }

    @Test
    void shouldSampleWeightedConcurrently() {

        double casesPassed = 0;
        var validCases = 0;
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNodes(List.of(idFunction.of("x")))
            .relationshipWeightProperty("distance")
            .concurrency(4)
            .samplingRatio(0.22)
            .restartProbability(0.01).build();
        var graph = getGraph(config);

        for (int i = 0; i < 250; i++) {
            var rwr = new RandomWalkWithRestarts(config);
            var nodes = rwr.compute(graph, ProgressTracker.NULL_TRACKER);
            if (rwr.startNodesUsed().contains(idFunction.of("x1")) || rwr
                .startNodesUsed()
                .contains(idFunction.of("x2"))) {
                continue;
            }
            validCases++;

            assertThat(nodes.cardinality()).isBetween(3L, 4L);
            assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("x1")))).isFalse();

            if (nodes.get(graph.toMappedNodeId(idFunction.of("x"))) &&
                !nodes.get(graph.toMappedNodeId(idFunction.of("x2"))) &&
                nodes.get(graph.toMappedNodeId(idFunction.of("x3")))
            ) {
                casesPassed++;
            }
        }
        // the analysis from single threaded case can be partially repeated. it takes 51 walks to get below 0.05/(4 ^ 2),
        // so the expectation would be (200 / 202) ** (51 * 4) , however the result is closer to (200 / 202) ** (51 * 2),
        // and its unclear exactly why.
        // this may be due to the fastest thread picking a new startnode before the other threads finish and therefore the
        // other threads have less time to find the improbable node x2
        assertThat(casesPassed / validCases).isCloseTo(
            Math.pow(200.0 / 202, 51.0 * config.concurrency() / 2),
            Offset.offset(0.33)
        );
    }

    @Test
    void shouldSampleWithFiltering() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNodes(List.of(idFunction.of("e")))
            .nodeLabels(List.of("M", "X"))
            .relationshipTypes(List.of("R1"))
            .samplingRatio(0.5)
            .restartProbability(0.1)
            .build();

        var rwr = new RandomWalkWithRestarts(config);
        var graph = getGraph(config);
        var nodes = rwr.compute(graph, ProgressTracker.NULL_TRACKER);

        assertThat(nodes.cardinality()).isEqualTo(3);

        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("e")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("f")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("g")))).isTrue();
    }

    @Test
    void shouldRestartOnDeadEnd() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .nodeLabels(List.of("Z"))
            .relationshipTypes(List.of("R1"))
            .startNodes(List.of(idFunction.of("x")))
            .samplingRatio(0.999999999)
            .restartProbability(0.0000000001)
            .build();

        var rwr = new RandomWalkWithRestarts(config);
        var nodes = rwr.compute(getGraph(config), ProgressTracker.NULL_TRACKER);

        assertThat(nodes.cardinality()).isEqualTo(4);
    }

    @Test
    void shouldBeDeterministic() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .samplingRatio(0.5)
            .startNodes(List.of(idFunction.of("a")))
            .restartProbability(0.1)
            .concurrency(1)
            .randomSeed(42L)
            .build();

        var rwr = new RandomWalkWithRestarts(config);

        var nodes1 = rwr.compute(getGraph(config), ProgressTracker.NULL_TRACKER);
        var nodes2 = rwr.compute(getGraph(config), ProgressTracker.NULL_TRACKER);

        assertThat(nodes1.cardinality()).isEqualTo(nodes2.cardinality());
        for (int i = 0; i < nodes1.size(); i++) {
            assertThat(nodes1.get(i)).isEqualTo(nodes2.get(i));
        }
    }

    @Test
    void shouldNotExploreNewStartNode() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNodes(List.of(idFunction.of("x")))
            .samplingRatio(4.0 / graphStore.nodeCount() + 0.001)
            .restartProbability(0.1)
            .build();

        var rwr = new RandomWalkWithRestarts(config);
        var graph = getGraph(config);
        var nodes = rwr.compute(graph, ProgressTracker.NULL_TRACKER);

        assertThat(nodes.cardinality()).isEqualTo(4);

        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("x")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("x1")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("x2")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("x3")))).isTrue();
    }

    @Test
    void shouldExploreNewStartNode() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNodes(List.of(idFunction.of("x")))
            .samplingRatio(5.0 / graphStore.nodeCount() + 0.001)
            .restartProbability(0.1)
            .build();

        var rwr = new RandomWalkWithRestarts(config);
        var nodes = rwr.compute(getGraph(config), ProgressTracker.NULL_TRACKER);

        assertThat(nodes.cardinality()).isGreaterThan(4);
    }

    @Test
    void shouldUseMultipleStartNodes() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNodes(List.of(idFunction.of("x"), idFunction.of("a"), idFunction.of("h"), idFunction.of("j")))
            .samplingRatio(1)
            .restartProbability(0.05)
            .build();

        var rwr = new RandomWalkWithRestarts(config);
        var nodes = rwr.compute(getGraph(config), ProgressTracker.NULL_TRACKER);

        assertThat(nodes.cardinality()).isEqualTo(14);
    }

    @Test
    void shouldSampleWithStratification() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNodes(List.of(idFunction.of("a")))
            .samplingRatio(0.5)
            .restartProbability(0.1)
            .randomSeed(42L)
            .concurrency(1)
            .nodeLabelStratification(true)
            .build();

        var rwr = new RandomWalkWithRestarts(config);
        var graph = getGraph(config);
        var nodes = rwr.compute(graph, ProgressTracker.NULL_TRACKER);

        assertThat(nodes.cardinality()).isEqualTo(8);

        var expectedLabelCounts = Map.of(
            Set.of(NodeLabel.of("X")), 1L,
            Set.of(NodeLabel.of("Z")), 2L,
            Set.of(NodeLabel.of("M")), 3L,
            Set.of(NodeLabel.of("N")), 2L
        );
        var labelCounts = new HashMap<Set<NodeLabel>, Long>();
        for (long nodeId = 0; nodeId < nodes.size(); nodeId++) {
            if (!nodes.get(nodeId)) {
                continue;
            }

            var labelSet = new HashSet<>(graph.nodeLabels(nodeId));
            labelCounts.put(labelSet, 1L + labelCounts.getOrDefault(labelSet, 0L));
        }

        assertThat(labelCounts).isEqualTo(expectedLabelCounts);
    }
}
