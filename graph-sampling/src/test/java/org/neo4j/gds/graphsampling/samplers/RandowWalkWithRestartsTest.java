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

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfigImpl;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class RandowWalkWithRestartsTest {

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
        ", (x)-[:R1 {distance: 30000.0} ]->(x3)" +
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
            Optional.empty()
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
        var nodes = rwr.sampleNodes(graph);

        assertThat(nodes.cardinality()).isEqualTo(7);

        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("a")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("b")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("c")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("d")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("e")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("f")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("g")))).isTrue();
    }

    @RepeatedTest(1000)
    void shouldSampleWeighted() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNodes(List.of(idFunction.of("x")))
            .relationshipWeightProperty("distance")
            .samplingRatio(0.22)
            .restartProbability(0.01)
            .build();

        var rwr = new RandomWalkWithRestarts(config);
        var graph = getGraph(config);
        var nodes = rwr.sampleNodes(graph);

        assertThat(nodes.cardinality()).isBetween(3L, 4L);

        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("x")))).isTrue();
        assertThat(nodes.get(graph.toMappedNodeId(idFunction.of("x3")))).isTrue();
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
        var nodes = rwr.sampleNodes(graph);

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
        var nodes = rwr.sampleNodes(getGraph(config));

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

        var nodes1 = rwr.sampleNodes(getGraph(config));
        var nodes2 = rwr.sampleNodes(getGraph(config));

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
        var nodes = rwr.sampleNodes(graph);

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
        var nodes = rwr.sampleNodes(getGraph(config));

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
        var nodes = rwr.sampleNodes(getGraph(config));

        assertThat(nodes.cardinality()).isEqualTo(14);
    }
}
